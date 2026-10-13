# requirements:
# psycopg[binary]
# pypdf

import io
import logging
from pathlib import Path

import psycopg
from psycopg import sql
from pypdf import PdfReader

from f.common_logic.db_operations import conninfo, postgresql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    pdfs: list,
    db: postgresql = None,
    db_table_name: str = None,
    text_dir: str = None,
):
    """Convert PDFs to plain text files, reusing cached extractions.

    Each PDF is extracted with pypdf to a `.txt` file (next to the PDF by
    default, or in `text_dir`). A text file that is at least as recent as its
    PDF is considered cached and extraction is skipped, so re-runs never
    re-parse unchanged files. PDFs with no extractable text layer (image-only
    scans) or that fail to parse are recorded as per-document errors without
    stopping the batch.

    Parameters
    ----------
    pdfs : list of dict or list of str
        Documents to extract, as returned by gazettes_download
        ([{"_id", "url", "path"}, ...]). Plain path strings are also accepted
        (state updates then require no db arguments).
    db : postgresql, optional
        A dictionary containing database connection parameters; when provided,
        the state table is updated.
    db_table_name : str, optional
        Detections table name; state table `<db_table_name>__documents` is
        updated with text extraction results.
    text_dir : str, optional
        Directory for extracted text files. Default: alongside each PDF.

    Returns
    -------
    dict
        {
            "texts": [{"_id", "url", "pdf_path", "text_path", "cached", "method"}, ...],
            "errors": [{"_id", "url", "error"}, ...],
        }
    """
    update_state = db is not None and db_table_name is not None
    state_table = f"{db_table_name.lower()[:54]}__documents" if update_state else None

    texts = []
    errors = []

    conn = (
        psycopg.connect(conninfo(db), autocommit=True) if update_state else _NullConn()
    )
    try:
        for item in pdfs or []:
            if isinstance(item, str):
                item = {"path": item}
            pdf_path = Path(item["path"])
            doc_id = item.get("_id")
            url = item.get("url")
            text_path = _text_path(pdf_path, text_dir)

            if _is_cached(pdf_path, text_path):
                logger.info(f"Text extraction cached, skipping: {text_path.name}")
                texts.append(
                    {
                        "_id": doc_id,
                        "url": url,
                        "pdf_path": str(pdf_path),
                        "text_path": str(text_path),
                        "cached": True,
                        "method": "cached",
                    }
                )
                _mark(conn, state_table, doc_id, "text_ready")
                continue

            try:
                extracted = extract_pdf_text(pdf_path)
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Failed to extract text from {pdf_path}: {error_msg}")
                errors.append({"_id": doc_id, "url": url, "error": error_msg})
                _mark(conn, state_table, doc_id, "error", error=error_msg)
                continue

            text_path.parent.mkdir(parents=True, exist_ok=True)
            text_path.write_text(extracted, encoding="utf-8")
            logger.info(f"Wrote text extraction: {text_path}")
            texts.append(
                {
                    "_id": doc_id,
                    "url": url,
                    "pdf_path": str(pdf_path),
                    "text_path": str(text_path),
                    "cached": False,
                    "method": "pypdf",
                }
            )
            _mark(conn, state_table, doc_id, "text_ready")
    finally:
        conn.close()

    return {"texts": texts, "errors": errors}


def extract_pdf_text(pdf_path: Path) -> str:
    """Extract the full text of a PDF with pypdf.

    Parameters
    ----------
    pdf_path : pathlib.Path
        Path to the PDF file.

    Returns
    -------
    str
        Concatenated page texts separated by newlines.

    Raises
    ------
    Exception
        Propagated from pypdf for unreadable/corrupt PDFs.
    ValueError
        If the PDF contains no extractable text at all.
    """
    with Path(pdf_path).open("rb") as f:
        reader = PdfReader(io.BytesIO(f.read()))
        pages = [(page.extract_text() or "") for page in reader.pages]
    text = "\n".join(pages)
    if not text.strip():
        raise ValueError(
            f"No extractable text found in {pdf_path} (image-only or empty PDF)."
        )
    return text


def _text_path(pdf_path: Path, text_dir: str = None) -> Path:
    """Return the cache path for a PDF's extracted text."""
    base = Path(text_dir) if text_dir else pdf_path.parent
    return base / (pdf_path.stem + ".txt")


def _is_cached(pdf_path: Path, text_path: Path) -> bool:
    """True when a text file exists and is at least as new as its PDF."""
    if not text_path.exists():
        return False
    return text_path.stat().st_mtime >= pdf_path.stat().st_mtime


class _NullConn:
    """No-op stand-in used when state updates are disabled."""

    def close(self):
        pass

    def cursor(self):
        raise RuntimeError("No database connection available.")


def _mark(conn, state_table, doc_id, status, error=None):
    """Update a document row's processing status in the state table."""
    if not state_table or not doc_id:
        return
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                UPDATE {table}
                SET status = %s, error = %s
                WHERE _id = %s
                """
            ).format(table=sql.Identifier(state_table)),
            (status, error, doc_id),
        )
