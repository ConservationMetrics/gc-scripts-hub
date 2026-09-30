# requirements:
# psycopg[binary]
# pyproj
# requests

"""Save GBIF occurrence facet counts for an area into PostgreSQL."""

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple
from urllib.parse import quote

import requests
from psycopg import connect, sql

from f.common_logic.db_operations import conninfo, postgresql
from f.common_logic.geo_utils import bounding_box_to_wkt

_API = "https://api.gbif.org/v1"
_FACET_LIMIT = 1000
_HEADERS = {
    "Accept": "application/json",
    "User-Agent": (
        "GuardianConnector GBIF connector "
        "(https://github.com/ConservationMetrics/gc-scripts-hub)"
    ),
}
_MAX_AREA_KM2 = 35_000
_REGISTRY_FIELDS = {
    "dataset": "title",
    "organization": "title",
    "species": "scientificName",
}
_REGISTRY_TIMEOUT = (5, 20)
_REGISTRY_WORKERS = 8
_SEARCH_TIMEOUT = (10, 120)


def _configure_logging() -> None:
    """Send each log line out immediately so a slow GBIF call is visible."""
    logging.basicConfig(level=logging.INFO)
    for handler in logging.getLogger().handlers:
        if getattr(handler, "_flushes_each_record", False):
            continue
        emit = handler.emit

        def flushing_emit(record, emit=emit, handler=handler):
            emit(record)
            handler.flush()

        handler.emit = flushing_emit
        handler._flushes_each_record = True


_configure_logging()
logger = logging.getLogger(__name__)


class _Facet(NamedTuple):
    suffix: str
    parameter: str
    label_column: str
    registry: str | None


_FACETS = (
    _Facet("datasets", "datasetKey", "name", "dataset"),
    _Facet("publishers", "publishingOrg", "name", "organization"),
    _Facet("years", "year", "year", None),
    _Facet("species", "speciesKey", "name", "species"),
    _Facet("basis_of_record", "basisOfRecord", "type", None),
)
_COLUMN_TYPES = {"name": "TEXT", "count": "BIGINT", "year": "INTEGER", "type": "TEXT"}
_MAX_TABLE_NAME_LENGTH = 63 - max(len(facet.suffix) + 1 for facet in _FACETS)


def main(bounding_box: list | str, db: postgresql, db_table_name: str) -> dict:
    """Replace facet-count tables for one GBIF bounding box.

    Parameters
    ----------
    bounding_box : list or str
        ``[[west, south], [east, north]]`` in longitude/latitude order.
    db : postgresql
        Database connection resource.
    db_table_name : str
        Occurrence table name. Counts are written to ``{name}_datasets``,
        ``{name}_publishers``, ``{name}_years``, ``{name}_species``, and
        ``{name}_basis_of_record``.
    """
    table_name = _validate_table_name(db_table_name)
    logger.info("Starting GBIF statistics for %s.", table_name)
    wkt = bounding_box_to_wkt(bounding_box, max_area_km2=_MAX_AREA_KM2)
    logger.info("GBIF statistics geometry: %s", wkt)
    occurrence_count, tables = _collect(wkt)
    summary = {
        "occurrence_count": occurrence_count,
        **{facet.suffix: len(rows) for facet, rows in tables},
    }
    logger.info(
        "GBIF statistics found %d occurrences: %s.",
        occurrence_count,
        {name: summary[name] for name in summary if name != "occurrence_count"},
    )
    _replace_tables(db, table_name, tables)
    logger.info("Finished GBIF statistics for %s.", table_name)
    return summary


def _validate_table_name(db_table_name: str) -> str:
    """Return a lowercase table name that can take every statistics suffix."""
    if (
        not isinstance(db_table_name, str)
        or not db_table_name
        or len(db_table_name) > _MAX_TABLE_NAME_LENGTH
        or "/" in db_table_name
        or "\\" in db_table_name
    ):
        raise ValueError(
            "db_table_name must be a non-empty table name of at most "
            f"{_MAX_TABLE_NAME_LENGTH} characters."
        )
    return db_table_name.lower()


def _collect(wkt: str) -> tuple[int, list[tuple[_Facet, list[tuple]]]]:
    """Fetch every facet, then resolve dataset, publisher, and species names."""
    with ThreadPoolExecutor(max_workers=len(_FACETS)) as executor:
        pages = list(
            executor.map(lambda facet: _facet_counts(wkt, facet.parameter), _FACETS)
        )
    occurrence_counts = [count for count, _entries in pages]
    if len(set(occurrence_counts)) > 1:
        logger.warning(
            "GBIF occurrence counts differed across facet queries: %s.",
            occurrence_counts,
        )
    lookups = list(
        dict.fromkeys(
            (facet.registry, name)
            for facet, (_count, entries) in zip(_FACETS, pages, strict=True)
            if facet.registry
            for name, _entry_count in entries
        )
    )
    logger.info("Resolving %d GBIF registry names.", len(lookups))
    resolved = _resolve_names(lookups)
    return max(occurrence_counts, default=0), [
        (facet, _rows(facet, entries, resolved))
        for facet, (_count, entries) in zip(_FACETS, pages, strict=True)
    ]


def _facet_counts(wkt: str, facet: str) -> tuple[int, list[tuple[str, int]]]:
    """Page one occurrence facet until GBIF runs out of values."""
    collected: list[tuple[str, int]] = []
    seen: set[str] = set()
    offset = 0
    occurrence_count = 0
    while True:
        logger.info("Requesting GBIF %s facet at offset %d.", facet, offset)
        response = requests.get(
            f"{_API}/occurrence/search",
            params={
                "geometry": wkt,
                "limit": 0,
                "facet": facet,
                "facetLimit": _FACET_LIMIT,
                "facetOffset": offset,
            },
            headers=_HEADERS,
            timeout=_SEARCH_TIMEOUT,
        )
        if not response.ok:
            logger.error(
                "GBIF %s facet request failed: HTTP %s.",
                facet,
                response.status_code,
            )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("GBIF occurrence search returned invalid JSON.") from exc
        if not isinstance(payload, dict):
            raise TypeError("GBIF occurrence search returned invalid JSON.")
        if offset == 0:
            count = payload.get("count")
            occurrence_count = (
                count if isinstance(count, int) and not isinstance(count, bool) else 0
            )
        batch = _facet_batch(payload)
        fresh = []
        for item in batch:
            parsed = _facet_entry(item)
            # A repeated page means facetOffset was ignored. Stop rather than loop.
            if parsed is None or parsed[0] in seen:
                continue
            seen.add(parsed[0])
            fresh.append(parsed)
        collected.extend(fresh)
        logger.info(
            "GBIF %s facet offset %d returned %d values (%d collected).",
            facet,
            offset,
            len(fresh),
            len(collected),
        )
        if len(batch) < _FACET_LIMIT or not fresh:
            return occurrence_count, collected
        offset += _FACET_LIMIT


def _facet_batch(payload: dict) -> list:
    facets = payload.get("facets") or []
    if not facets:
        return []
    first = facets[0]
    if not isinstance(first, dict):
        raise TypeError("GBIF occurrence search returned an invalid facet page.")
    counts = first.get("counts") or []
    if not isinstance(counts, list):
        raise TypeError("GBIF occurrence search returned an invalid facet page.")
    return counts


def _facet_entry(item: object) -> tuple[str, int] | None:
    if not isinstance(item, dict):
        return None
    name = item.get("name")
    count = item.get("count")
    if (
        not isinstance(name, str)
        or isinstance(count, bool)
        or not isinstance(count, int)
        or count < 0
    ):
        return None
    name = name.strip()
    if not name:
        return None
    return name, count


def _resolve_names(lookups: list[tuple[str, str]]) -> dict[tuple[str, str], str]:
    """Resolve Registry titles and scientific names, skipping failed lookups."""
    if not lookups:
        return {}
    resolved: dict[tuple[str, str], str] = {}
    total = len(lookups)
    with ThreadPoolExecutor(max_workers=min(_REGISTRY_WORKERS, total)) as pool:
        names = pool.map(lambda lookup: _registry_name(*lookup), lookups)
        for index, (lookup, name) in enumerate(
            zip(lookups, names, strict=True), start=1
        ):
            if name:
                resolved[lookup] = name
            if index == 1 or index == total or index % 50 == 0:
                logger.info("Resolved %d of %d GBIF registry names.", index, total)
    return resolved


def _registry_name(kind: str, key: str) -> str | None:
    """Return one Registry label, or ``None`` when the lookup does not succeed."""
    field = _REGISTRY_FIELDS[kind]
    try:
        response = requests.get(
            f"{_API}/{kind}/{quote(key, safe='')}",
            headers=_HEADERS,
            timeout=_REGISTRY_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        logger.info("Could not resolve GBIF %s %s: %s", kind, key, exc)
        return None
    if not isinstance(payload, dict):
        return None
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        logger.info("GBIF %s %s returned no %s.", kind, key, field)
        return None
    return value.strip()


def _rows(
    facet: _Facet,
    entries: list[tuple[str, int]],
    resolved: dict[tuple[str, str], str],
) -> list[tuple]:
    """Build sorted ``(label, count)`` rows, falling back to the GBIF key."""
    rows = []
    unresolved = 0
    for name, count in entries:
        if facet.label_column == "year":
            if not name.isdigit():
                logger.warning("Skipping GBIF year facet value %r.", name)
                continue
            rows.append((int(name), count))
            continue
        label = resolved.get((facet.registry, name)) if facet.registry else name
        if facet.registry and not label:
            unresolved += 1
            label = name
        rows.append((label, count))
    if unresolved:
        logger.warning(
            "GBIF %s statistics left %d of %d names unresolved.",
            facet.suffix,
            unresolved,
            len(entries),
        )
    if facet.label_column == "year":
        rows.sort(key=lambda row: -row[0])
    else:
        rows.sort(key=lambda row: (-row[1], row[0]))
    return rows


def _replace_tables(
    db: postgresql, table_name: str, tables: list[tuple[_Facet, list[tuple]]]
) -> None:
    """Replace every statistics table in one transaction."""
    logger.info("Replacing GBIF statistics tables for %s.", table_name)
    with (
        connect(conninfo(db), autocommit=True) as connection,
        connection.transaction(),
        connection.cursor() as cursor,
    ):
        for facet, rows in tables:
            qualified = f"{table_name}_{facet.suffix}"
            _replace_table(cursor, qualified, facet, rows)
            logger.info(
                "Saved %d GBIF %s rows to %s.", len(rows), facet.suffix, qualified
            )


def _replace_table(cursor, table_name: str, facet: _Facet, rows: list[tuple]) -> None:
    columns = (
        (facet.label_column, _COLUMN_TYPES[facet.label_column]),
        ("count", _COLUMN_TYPES["count"]),
    )
    table = sql.Identifier(table_name)
    cursor.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            table,
            sql.SQL(", ").join(
                sql.SQL("{} {} NOT NULL").format(
                    sql.Identifier(column), sql.SQL(column_type)
                )
                for column, column_type in columns
            ),
        )
    )
    cursor.execute(sql.SQL("TRUNCATE {}").format(table))
    if not rows:
        return
    with cursor.copy(
        sql.SQL("COPY {} ({}) FROM STDIN").format(
            table,
            sql.SQL(", ").join(sql.Identifier(column) for column, _type in columns),
        )
    ) as copy:
        for row in rows:
            copy.write_row(row)
