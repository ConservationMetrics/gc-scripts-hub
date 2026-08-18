# requirements:
# psycopg[binary]
# sensingcluespy~=0.2.3

import logging
from pathlib import Path
from typing import Any

from sensingcluespy.api_calls import SensingClues
from sensingcluespy.src.helper_functions import make_query

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_data_to_file
from f.connectors.csv.csv_to_postgres import main as save_csv_to_postgres

_PAGE_LENGTH = 200

# Core fields set on every transformed row. Observation ``attributes`` that
# collide with these (e.g. ``fileName``, ``tags``) are skipped.
_CORE_KEYS = frozenset(
    {
        "_id",
        "entityType",
        "entityClass",
        "projectId",
        "projectName",
        "observationType",
        "observationClass",
        "when",
        "description",
        "tags",
        "agentName",
        "createdOn",
        "createdBy",
        "fileName",
        "conceptLabels",
        "conceptIds",
        "data_source",
        "dataset_name",
        "g__type",
        "g__coordinates",
    }
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    username: str,
    password: str,
    groups: list,
    db: postgresql,
    db_table_name: str,
    date_from: str | None = None,
    date_until: str | None = None,
    attachment_root: str = "/persistent-storage/datalake",
):
    """Fetch SensingClues observations and write them to the datalake and PostgreSQL.

    Parameters
    ----------
    username : str
        SensingClues Focus username.
    password : str
        SensingClues Focus password.
    groups : list
        Group names to query, e.g. ``["focus-project-1234"]``.
    db : postgresql
        Database connection configuration.
    db_table_name : str
        Database table name and datalake subdirectory.
    date_from : str, optional
        Inclusive start date (``YYYY-MM-DD``).
    date_until : str, optional
        Inclusive end date (``YYYY-MM-DD``).
    attachment_root : str
        Root directory for persisted files.
    """
    if isinstance(groups, str):
        groups = [groups]

    client = SensingClues(username, password)
    _validate_groups(client, groups)

    observations = download_observations(
        client, groups, date_from=date_from, date_until=date_until
    )
    write_observations(observations, db, db_table_name, attachment_root)


def _list_group_names(client: SensingClues) -> list[str]:
    """Return group names the account can see, from ``search/all/facets``.

    ``SensingClues.get_groups()`` is not used: the PyPI wheel for
    ``sensingcluespy==0.2.3`` omits the extractor JSON files that method
    needs, so we parse the facets payload ourselves.
    """
    query = make_query(data_type=["observation", "track"])
    payload = client._api_call("post", "search/all/facets", query).json()
    values = (
        payload.get("facets", {}).get("dataSources", {}).get("facetValues") or []
    )
    return [entry["name"] for entry in values if entry.get("name")]


def _validate_groups(client: SensingClues, groups: list[str]) -> None:
    """Raise if any requested group is not in the account's available groups."""
    names = set(_list_group_names(client))
    missing = [group for group in groups if group not in names]
    if missing:
        raise ValueError(
            f"Unknown group(s): {missing}. Available: {sorted(names)}"
        )


def download_observations(
    client: SensingClues,
    groups: list[str],
    date_from: str | None = None,
    date_until: str | None = None,
) -> list[dict]:
    """Page through ``search/all/results`` and return raw observation dicts.

    Uses ``sensingcluespy`` for login, query construction, and HTTP error
    mapping, but parses the raw JSON rather than ``get_observations()``.
    That extractor explodes one row per concept and drops attributes.

    ``_api_call`` is private; this coupling is pinned via ``sensingcluespy~=0.2.3``.
    """
    filters: dict[str, Any] = {}
    if date_from:
        filters["date_from"] = date_from
    if date_until:
        filters["date_until"] = date_until

    observations: list[dict] = []
    page = 0
    total: int | None = None

    while True:
        query = make_query(
            groups=groups,
            data_type=["observation"],
            page_nbr=page,
            page_length=_PAGE_LENGTH,
            **filters,
        )
        payload = client._api_call("post", "search/all/results", query).json()
        batch = payload.get("results") or []
        if total is None:
            total = int(payload.get("total") or 0)
            logger.info("Scope %s contains %s observations.", groups, total)

        if not batch:
            break

        observations.extend(batch)
        logger.info(
            "[%s] Fetched %s of %s observations",
            ", ".join(groups),
            len(observations),
            total,
        )

        if len(observations) >= total or len(batch) < _PAGE_LENGTH:
            break
        page += 1

    logger.info(
        "[%s] Downloaded %s total observations.",
        ", ".join(groups),
        len(observations),
    )
    return observations


def write_observations(
    observations: list[dict],
    db: postgresql,
    db_table_name: str,
    attachment_root: str,
) -> None:
    """Save raw JSON + CSV to the datalake and write rows to PostgreSQL."""
    save_path = Path(attachment_root) / db_table_name

    if not observations:
        logger.warning(
            "No observations returned; skipping database write for table: [%s]",
            db_table_name,
        )
        return

    save_data_to_file(
        observations,
        f"{db_table_name}_observations",
        save_path,
        file_type="json",
    )

    transformed = transform_observations(observations)
    save_data_to_file(transformed, db_table_name, save_path, file_type="csv")
    save_csv_to_postgres(
        db,
        db_table_name,
        str(Path(db_table_name) / f"{db_table_name}.csv"),
        attachment_root,
        delete_csv_file=False,
        id_column="_id",
        use_mapping_table=True,
    )
    logger.info(
        "SensingClues observations written to database table: [%s]",
        db_table_name,
    )


def transform_observations(results: list[dict]) -> list[dict]:
    """Flatten raw Focus search results to one row per observation.

    Parameters
    ----------
    results : list of dict
        Items from ``search/all/results`` ``results`` array.

    Returns
    -------
    list of dict
        Rows ready for ``save_data_to_file`` / ``StructuredDBWriter``.
    """
    rows = []
    for result in results:
        content = (result.get("extracted") or {}).get("content") or []
        headers = _content_block(content, "headers")
        observation = _content_block(content, "Observation")
        agent = observation.get("agent") or {}
        concepts = observation.get("concepts") or []

        row = {
            "_id": headers.get("entityId") or result.get("id"),
            "entityType": headers.get("entityType"),
            "entityClass": headers.get("entityClass"),
            "projectId": headers.get("projectId"),
            "projectName": headers.get("projectName"),
            "observationType": observation.get("observationType"),
            "observationClass": observation.get("observationClass"),
            "when": observation.get("when"),
            "description": observation.get("description"),
            "tags": observation.get("tags") or [],
            "agentName": agent.get("agentName") or "",
            "createdOn": headers.get("createdOn"),
            "createdBy": headers.get("createdBy"),
            "fileName": headers.get("fileName"),
            "conceptLabels": [c.get("label") for c in concepts if c.get("label")],
            "conceptIds": [c.get("conceptId") for c in concepts if c.get("conceptId")],
            "data_source": "SensingClues",
            "dataset_name": headers.get("projectName"),
        }

        where = observation.get("where") or {}
        if where.get("type") and where.get("coordinates") is not None:
            row["g__type"] = where["type"]
            row["g__coordinates"] = where["coordinates"]

        for attr in observation.get("attributes") or []:
            key = attr.get("key")
            if not key:
                continue
            if key in row or key in _CORE_KEYS:
                logger.debug("Skipping attribute %r; core field wins", key)
                continue
            row[key] = attr.get("value")

        rows.append(row)

    logger.info("Transformed %s observation(s).", len(rows))
    return rows


def _content_block(content: list, key: str) -> dict:
    """Return the first dict in ``extracted.content`` that contains ``key``."""
    for item in content:
        if isinstance(item, dict) and key in item:
            return item[key]
    return {}
