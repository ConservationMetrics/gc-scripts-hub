# requirements:
# psycopg[binary]
# requests

import hashlib
import logging
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlparse

import psycopg
import requests
from psycopg import sql

from f.common_logic.db_operations import conninfo, postgresql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; GuardianConnector/1.0; +https://guardianconnector.net)"
PDF_MAGIC = b"%PDF"


def main(
    docs: list,
    db: postgresql,
    db_table_name: str,
    cache_dir: str = "/persistent-storage/datalake/gazettes/pdf",
    check_updates: bool = True,
    user_agent: str = DEFAULT_USER_AGENT,
    request_timeout: int = 60,
):
    """Download PDFs for scraped documents into a local cache, skipping known files.

    Files are stored under `cache_dir` using a URL-derived filename (sanitized
    basename plus a short hash of the URL), so the same URL always maps to the
    same cache entry and is never re-downloaded. When `check_updates` is set, a
    HEAD request compares ETag/Content-Length against the cached copy so that
    same-URL replacements (common on government sites) are detected and
    re-downloaded.

    Parameters
    ----------
    docs : list of dict
        Documents to download, as returned by gazettes_scrape
        ([{"_id", "url", "link_text", ...}]).
    db : postgresql
        A dictionary containing database connection parameters.
    db_table_name : str
        Detections table name; the state table `<db_table_name>__documents` is
        updated with download results.
    cache_dir : str, optional
        Directory for the PDF cache.
    check_updates : bool, optional
        HEAD-check cached files for same-URL updates before skipping download.
        Default: True.
    user_agent : str, optional
        User-Agent header sent with requests.
    request_timeout : int, optional
        Timeout in seconds for HTTP requests.

    Returns
    -------
    dict
        {
            "new_pdfs": [{"_id", "url", "path", "changed": bool}, ...],
            "cached": int,
            "errors": [{"_id", "url", "error"}, ...],
        }
        `new_pdfs` contains only files that were downloaded now or were already
        present without needing re-download (i.e. never-processed docs); docs
        whose cached copy is current are NOT returned, so downstream stages
        process only updates.
    """
    cache_dir = cache_dir or "/persistent-storage/datalake/gazettes/pdf"
    user_agent = user_agent or DEFAULT_USER_AGENT
    request_timeout = request_timeout or 60
    check_updates = True if check_updates is None else bool(check_updates)

    state_table = f"{db_table_name.lower()[:54]}__documents"
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    new_pdfs = []
    errors = []
    cached = 0

    with psycopg.connect(conninfo(db), autocommit=True) as conn:
        for doc in docs or []:
            url = doc["url"]
            doc_id = doc.get("_id")
            local_file = cache_path / _url_filename(url)

            if local_file.exists():
                status, _, _ = _get_state_meta(conn, state_table, doc_id)
                changed = check_updates and _remote_changed(
                    conn, state_table, doc_id, url, local_file, user_agent, request_timeout
                )
                if changed:
                    logger.info(f"Cached PDF is stale (same URL, new content): {local_file.name}")
                elif status in ("new", "error"):
                    # File is current but the document never finished processing
                    # (e.g. retried via include_errored): pass it downstream.
                    logger.info(
                        f"PDF cached but status is '{status}'; re-processing: {local_file.name}"
                    )
                    new_pdfs.append(
                        {"_id": doc_id, "url": url, "path": str(local_file), "changed": False}
                    )
                    continue
                else:
                    logger.info(f"PDF already cached, skipping download: {local_file.name}")
                    cached += 1
                    continue

            try:
                size, etag = _download_pdf(url, local_file, user_agent, request_timeout)
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Failed to download {url}: {error_msg}")
                errors.append({"_id": doc_id, "url": url, "error": error_msg})
                _mark_state(conn, state_table, doc_id, "error", error=error_msg)
                continue

            _mark_state(
                conn,
                state_table,
                doc_id,
                "downloaded",
                local_path=str(local_file),
                etag=etag,
                content_length=size,
            )
            new_pdfs.append(
                {"_id": doc_id, "url": url, "path": str(local_file), "changed": True}
            )
            logger.info(f"Downloaded {url} -> {local_file}")

    return {"new_pdfs": new_pdfs, "cached": cached, "errors": errors}


def _url_filename(url: str) -> str:
    """Derive a stable, sanitized cache filename from a document URL.

    The basename from the URL path is sanitized and suffixed with the first 8
    characters of the URL's MD5 hash to avoid collisions between different
    URLs that share a basename.

    Parameters
    ----------
    url : str
        Absolute document URL.

    Returns
    -------
    str
        Filename ending in .pdf.
    """
    path = unquote(urlparse(url).path)
    base = Path(path).name or "document.pdf"
    stem = Path(base).stem
    ext = Path(base).suffix or ".pdf"
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-._") or "document"
    # Keep filenames bounded; the URL hash preserves uniqueness.
    stem = stem[:120]
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    return f"{stem}__{url_hash}{ext.lower() if ext.lower().endswith('.pdf') else '.pdf'}"


def _download_pdf(
    url: str, local_file: Path, user_agent: str, request_timeout: int
) -> tuple:
    """Stream a PDF to `local_file` atomically, verifying it looks like a PDF.

    Parameters
    ----------
    url : str
        Document URL.
    local_file : pathlib.Path
        Destination path.
    user_agent : str
        User-Agent header value.
    request_timeout : int
        Timeout in seconds.

    Returns
    -------
    tuple
        (size_bytes, etag_or_None)

    Raises
    ------
    requests.HTTPError
        On non-2xx responses.
    ValueError
        If the downloaded content is not a PDF.
    """
    headers = {"User-Agent": user_agent}
    with requests.get(
        url, headers=headers, timeout=request_timeout, stream=True
    ) as response:
        response.raise_for_status()
        etag = response.headers.get("ETag")
        fd, tmp_name = tempfile.mkstemp(dir=str(local_file.parent), suffix=".part")
        size = 0
        first_chunk = True
        try:
            with os.fdopen(fd, "wb") as f:
                for chunk in response.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    if first_chunk:
                        if not chunk.startswith(PDF_MAGIC):
                            raise ValueError(
                                f"Downloaded content for {url} is not a PDF."
                            )
                        first_chunk = False
                    f.write(chunk)
                    size += len(chunk)
            if size == 0:
                raise ValueError(f"Downloaded empty file for {url}.")
            os.replace(tmp_name, local_file)
        except Exception:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise
    return size, etag


def _remote_changed(
    conn,
    state_table: str,
    doc_id: str,
    url: str,
    local_file: Path,
    user_agent: str,
    request_timeout: int,
) -> bool:
    """Check whether a cached file is stale using HEAD metadata.

    Compares ETag first (preferred) and Content-Length second. If neither can
    be established, the file is considered unchanged (conservative default).

    Parameters
    ----------
    conn : psycopg.Connection
        Open database connection for reading stored metadata.
    state_table : str
        State table name.
    doc_id : str or None
        Document state row id.
    url : str
        Document URL.
    local_file : pathlib.Path
        Cached file.
    user_agent : str
        User-Agent header value.
    request_timeout : int
        Timeout in seconds.

    Returns
    -------
    bool
        True when the remote file appears to differ from the cached one.
    """
    _, stored_etag, stored_length = _get_state_meta(conn, state_table, doc_id)
    local_length = local_file.stat().st_size

    if stored_length is not None and stored_length != local_length:
        logger.info(f"Content-Length differs for {url}; will re-download.")
        return True

    if stored_etag:
        try:
            response = requests.head(
                url,
                headers={"User-Agent": user_agent},
                timeout=request_timeout,
                allow_redirects=True,
            )
            response.raise_for_status()
            remote_etag = response.headers.get("ETag")
            if remote_etag and remote_etag != stored_etag:
                logger.info(f"ETag changed for {url}; will re-download.")
                return True
        except Exception as e:
            logger.warning(
                f"HEAD check failed for {url} ({e}); trusting cached file."
            )
    return False


def _get_state_meta(conn, state_table: str, doc_id: str) -> tuple:
    """Read stored (status, etag, content_length) for a document row, if any."""
    if not doc_id:
        return None, None, None
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "SELECT status, etag, content_length FROM {table} WHERE _id = %s"
            ).format(table=sql.Identifier(state_table)),
            (doc_id,),
        )
        row = cursor.fetchone()
    return (row[0], row[1], row[2]) if row else (None, None, None)


def _mark_state(
    conn,
    state_table: str,
    doc_id: str,
    status: str,
    local_path: str = None,
    etag: str = None,
    content_length: int = None,
    error: str = None,
):
    """Update a document row's download status and metadata."""
    if not doc_id:
        return
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                UPDATE {table}
                SET status = %s,
                    local_path = COALESCE(%s, local_path),
                    etag = COALESCE(%s, etag),
                    content_length = COALESCE(%s, content_length),
                    error = %s
                WHERE _id = %s
                """
            ).format(table=sql.Identifier(state_table)),
            (status, local_path, etag, content_length, error, doc_id),
        )
