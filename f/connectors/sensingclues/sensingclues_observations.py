# requirements:
# psycopg[binary]
# sensingcluespy~=0.2.3
# setuptools<81
# numpy<2.5

import logging
from pathlib import Path
from typing import Any

from sensingcluespy.api_calls import SensingClues
from sensingcluespy.src.helper_functions import make_query

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_data_to_file
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres

_PAGE_LENGTH = 200
# Focus API group names are always this prefix plus the Cluey/Central identifier.
_GROUP_PREFIX = "focus-project-"

# Feature property keys, plus geometry columns written later by
# geojson_to_postgres. Observation ``attributes`` that collide with these
# (e.g. ``fileName``, ``tags``) are skipped.
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
    group_identifier: str,
    db: postgresql,
    db_table_name: str,
    date_from: str | None = None,
    date_until: str | None = None,
    attachment_root: str = "/persistent-storage/datalake",
):
    if not group_identifier.isdigit():
        raise ValueError(f"Group identifier must be numeric, got {group_identifier!r}.")

    group = f"{_GROUP_PREFIX}{group_identifier}"
    client = SensingClues(username, password)
    _validate_group(client, group)

    observations = download_observations(
        client, group, date_from=date_from, date_until=date_until
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
    values = payload.get("facets", {}).get("dataSources", {}).get("facetValues") or []
    return [entry["name"] for entry in values if entry.get("name")]


def _validate_group(client: SensingClues, group: str) -> None:
    """Raise if the requested group is not in the account's available groups."""
    names = set(_list_group_names(client))
    if group not in names:
        available = sorted(
            name.removeprefix(_GROUP_PREFIX)
            for name in names
            if name.removeprefix(_GROUP_PREFIX).isdigit()
        )
        raise ValueError(
            f"Unknown group identifier: {group.removeprefix(_GROUP_PREFIX)}. "
            f"Available: {available}"
        )


def download_observations(
    client: SensingClues,
    group: str,
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
            groups=[group],
            data_type=["observation"],
            page_nbr=page,
            page_length=_PAGE_LENGTH,
            **filters,
        )
        payload = client._api_call("post", "search/all/results", query).json()
        batch = payload.get("results") or []
        if total is None:
            total = int(payload.get("total") or 0)
            logger.info("Group %s contains %s observations.", group, total)

        if not batch:
            break

        observations.extend(batch)
        logger.info(
            "[%s] Fetched %s of %s observations",
            group,
            len(observations),
            total,
        )

        if len(observations) >= total or len(batch) < _PAGE_LENGTH:
            break
        page += 1

    logger.info(
        "[%s] Downloaded %s total observations.",
        group,
        len(observations),
    )
    return observations


def write_observations(
    observations: list[dict],
    db: postgresql,
    db_table_name: str,
    attachment_root: str,
) -> None:
    """Save raw JSON + GeoJSON to the datalake and write features to PostgreSQL."""
    save_path = Path(attachment_root) / db_table_name
    save_data_to_file(
        observations,
        f"{db_table_name}_observations",
        save_path,
        file_type="json",
    )

    geojson = transform_observations_to_geojson(observations)

    if geojson["features"]:
        save_data_to_file(geojson, db_table_name, save_path, file_type="geojson")
        save_geojson_to_postgres(
            db,
            db_table_name,
            str(Path(db_table_name) / f"{db_table_name}.geojson"),
            attachment_root,
            delete_geojson_file=False,
        )
        logger.info(
            "SensingClues observations written to database table: [%s]",
            db_table_name,
        )
    else:
        logger.warning(
            "No observations returned; skipping database write for table: [%s]",
            db_table_name,
        )


def transform_observations_to_geojson(results: list[dict]) -> dict:
    """Convert raw Focus search results into a GeoJSON FeatureCollection.

    Feature ``properties`` flatten headers, observation fields, and form
    ``attributes``. Ontology concepts collapse into ``conceptLabels`` /
    ``conceptIds``. Geometry comes from ``Observation.where``. Nested API
    payloads are omitted here; complete records are still written to the
    datalake as ``{db_table_name}_observations.json``.

    Parameters
    ----------
    results : list of dict
        Items from ``search/all/results`` ``results`` array.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection with flattened properties.
    """
    features = []
    for result in results:
        content = (result.get("extracted") or {}).get("content") or []
        headers = _content_block(content, "headers")
        observation = _content_block(content, "Observation")
        agent = observation.get("agent") or {}
        concepts = observation.get("concepts") or []

        properties = {
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

        geometry = None
        where = observation.get("where") or {}
        if where.get("type") and where.get("coordinates") is not None:
            geometry = {"type": where["type"], "coordinates": where["coordinates"]}

        for attr in observation.get("attributes") or []:
            key = attr.get("key")
            if not key:
                continue
            if key in properties or key in _CORE_KEYS:
                logger.debug("Skipping attribute %r; core field wins", key)
                continue
            properties[key] = attr.get("value")

        features.append(
            {
                "type": "Feature",
                "id": headers.get("entityId") or result.get("id"),
                "geometry": geometry,
                "properties": properties,
            }
        )

    logger.info("Formatted %s observation(s) as GeoJSON features.", len(features))
    return {"type": "FeatureCollection", "features": features}


def _content_block(content: list, key: str) -> dict:
    """Return the first dict in ``extracted.content`` that contains ``key``."""
    for item in content:
        if isinstance(item, dict) and key in item:
            return item[key]
    return {}
