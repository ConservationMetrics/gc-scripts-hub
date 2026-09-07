# requirements:
# beautifulsoup4
# lxml
# psycopg[binary]
# requests

import logging
import re
import uuid
from urllib.parse import urljoin, urlparse

import psycopg
import requests
from bs4 import BeautifulSoup
from psycopg import sql

from f.common_logic.db_operations import conninfo, postgresql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; GuardianConnector/1.0; +https://guardianconnector.net)"
)


def main(
    site_url: str,
    db: postgresql,
    db_table_name: str,
    pdf_selector: str = "#content a[href$='.pdf']",
    link_pattern: str = None,
    user_agent: str = DEFAULT_USER_AGENT,
    request_timeout: int = 30,
    include_errored: bool = False,
):
    """Scrape a page for PDF links and keep track of site updates in a state table.

    Fetches `site_url`, selects anchors matching `pdf_selector` (a CSS selector),
    resolves them to absolute URLs, and records them in a per-source state table
    (`<db_table_name>__documents`). Only links never seen before (or previously
    errored, when `include_errored` is set) are returned as "new", so scheduled
    runs process a government site's updates incrementally.

    Parameters
    ----------
    site_url : str
        The page to scrape (e.g. a government gazette listing page).
    db : postgresql
        A dictionary containing database connection parameters.
    db_table_name : str
        Name of the detections table this source feeds; the state table is
        derived from it as `<db_table_name>__documents`.
    pdf_selector : str, optional
        CSS selector used to find document links on the page. Default scopes
        to the main content area (WordPress nav menus carry unrelated PDF
        links on the GGMC site).
    link_pattern : str, optional
        Optional regex; when set, only hrefs matching this pattern (applied to
        the absolute URL) are kept.
    user_agent : str, optional
        User-Agent header sent with the request.
    request_timeout : int, optional
        Timeout in seconds for the HTTP request.
    include_errored : bool, optional
        When True, documents previously marked with status 'error' are returned
        again so downstream scripts can retry them.

    Returns
    -------
    dict
        {
            "new_docs": [{"_id": str, "url": str, "link_text": str}, ...],
            "stats": {"found": int, "new": int, "errored": int},
            "site_url": str,
            "db_table_name": str,
        }
    """
    pdf_selector = pdf_selector or "#content a[href$='.pdf']"
    user_agent = user_agent or DEFAULT_USER_AGENT
    request_timeout = request_timeout or 30

    html = _fetch_page(site_url, user_agent, request_timeout)
    docs = _extract_pdf_links(html, site_url, pdf_selector, link_pattern)

    state_table = _state_table_name(db_table_name)
    logger.info(
        f"Found {len(docs)} document link(s) on {site_url}; "
        f"comparing against state table [{state_table}]."
    )

    new_docs = _register_docs(conninfo(db), state_table, docs, include_errored)

    errored = [d for d in new_docs if d.get("status") == "error"]
    stats = {
        "found": len(docs),
        "new": len(new_docs),
        "errored": len(errored),
    }
    logger.info(f"State comparison result: {stats}")

    return {
        "new_docs": new_docs,
        "stats": stats,
        "site_url": site_url,
        "db_table_name": db_table_name,
    }


def _state_table_name(db_table_name: str) -> str:
    """Return the derived state table name for a given detections table name."""
    return f"{db_table_name.lower()[:54]}__documents"


def _fetch_page(site_url: str, user_agent: str, request_timeout: int) -> str:
    """Fetch a page with requests and return its HTML text.

    Parameters
    ----------
    site_url : str
        URL of the page to fetch.
    user_agent : str
        User-Agent header value.
    request_timeout : int
        Request timeout in seconds.

    Returns
    -------
    str
        Response body text.
    """
    headers = {"User-Agent": user_agent}
    response = requests.get(
        site_url, headers=headers, timeout=request_timeout, allow_redirects=True
    )
    response.raise_for_status()
    return response.text


def _extract_pdf_links(
    html: str, site_url: str, pdf_selector: str, link_pattern: str = None
) -> list:
    """Extract absolute document URLs from page HTML using a CSS selector.

    Parameters
    ----------
    html : str
        Page HTML.
    site_url : str
        Base URL used to resolve relative hrefs.
    pdf_selector : str
        CSS selector for candidate anchors.
    link_pattern : str, optional
        Optional regex filter applied to absolute URLs.

    Returns
    -------
    list of dict
        Unique docs sorted by URL: [{"_id", "url", "link_text"}].
    """
    soup = BeautifulSoup(html, "lxml")
    pattern = re.compile(link_pattern) if link_pattern else None

    seen = set()
    docs = []
    for anchor in soup.select(pdf_selector):
        href = anchor.get("href")
        if not href:
            continue
        url = urljoin(site_url, href.strip())
        if not urlparse(url).scheme in {"http", "https"}:
            logger.debug(f"Skipping non-http href: {href}")
            continue
        if pattern and not pattern.search(url):
            continue
        doc_id = str(uuid.uuid5(uuid.NAMESPACE_URL, url))
        if doc_id in seen:
            continue
        seen.add(doc_id)
        link_text = anchor.get("title") or anchor.get_text(strip=True)
        docs.append({"_id": doc_id, "url": url, "link_text": link_text or None})

    return sorted(docs, key=lambda d: d["url"])


def create_documents_state_table(cursor, table_name):
    cursor.execute(
        sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            _id character varying(36) NOT NULL PRIMARY KEY,
            url text NOT NULL,
            link_text text,
            page_url text,
            status text NOT NULL DEFAULT 'new',
            local_path text,
            etag text,
            content_length bigint,
            parse_status text,
            parse_notes text,
            error text,
            first_seen timestamptz NOT NULL DEFAULT now(),
            last_seen timestamptz NOT NULL DEFAULT now(),
            last_processed timestamptz
        );
    """).format(table=sql.Identifier(table_name))
    )


def _register_docs(
    db_connection_string: str, state_table: str, docs: list, include_errored: bool
) -> list:
    """Insert unseen docs into the state table and refresh last_seen for known ones.

    Parameters
    ----------
    db_connection_string : str
        psycopg connection string.
    state_table : str
        Fully derived state table name.
    docs : list of dict
        Docs extracted from the page (each with `_id`, `url`, `link_text`).
    include_errored : bool
        When True, docs whose stored status is 'error' are returned as new again.

    Returns
    -------
    list of dict
        The "new" docs (never seen, plus errored when include_errored),
        each annotated with its current stored status.
    """
    new_docs = []
    with psycopg.connect(db_connection_string, autocommit=True) as conn:
        with conn.cursor() as cursor:
            create_documents_state_table(cursor, state_table)

            cursor.execute(
                sql.SQL("SELECT _id, status FROM {table}").format(
                    table=sql.Identifier(state_table)
                )
            )
            existing = dict(cursor.fetchall())

            for doc in docs:
                stored = existing.get(doc["_id"])
                if stored is None:
                    cursor.execute(
                        sql.SQL(
                            """
                            INSERT INTO {table} (_id, url, link_text)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (_id) DO NOTHING
                            """
                        ).format(table=sql.Identifier(state_table)),
                        (doc["_id"], doc["url"], doc["link_text"]),
                    )
                    new = dict(doc)
                    new["status"] = "new"
                    new_docs.append(new)
                    logger.info(f"New document found: {doc['url']}")
                else:
                    cursor.execute(
                        sql.SQL(
                            """
                            UPDATE {table}
                            SET last_seen = now(),
                                link_text = COALESCE(%s, link_text)
                            WHERE _id = %s
                            """
                        ).format(table=sql.Identifier(state_table)),
                        (doc["link_text"], doc["_id"]),
                    )
                    if include_errored and stored == "error":
                        new = dict(doc)
                        new["status"] = stored
                        new_docs.append(new)

    return new_docs
