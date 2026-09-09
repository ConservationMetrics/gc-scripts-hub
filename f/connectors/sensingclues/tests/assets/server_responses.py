"""Mock SensingClues Focus API responses for testing.

Fixtures are captured from the public ``demo`` / ``demo`` account, then trimmed
to the fields and records the connector actually reads:

* identifier ``3494596`` (``focus-project-3494596``) — Demo Cluey Group (2 observations)
* identifier ``1234`` (``focus-project-1234``) — Demo Africa Upload (3 CSV-ingest observations)
* ``facets.json`` — ``search/all/facets`` payload used for group discovery
"""

import json
from pathlib import Path

_ASSETS = Path(__file__).parent

CLUEY_IDENTIFIER = "3494596"
AFRICA_IDENTIFIER = "1234"
CLUEY_GROUP = f"focus-project-{CLUEY_IDENTIFIER}"
AFRICA_GROUP = f"focus-project-{AFRICA_IDENTIFIER}"

# First Cluey observation: community work with flattened attributes + geometry
PRIMARY_ENTITY_ID = "O3494596-n9e770de501315aa5"
PRIMARY_PROJECT_NAME = "Demo Cluey Group"
PRIMARY_AGENT_NAME = "jankees"
PRIMARY_BENEFICIARIES_TOTAL = "47"
PRIMARY_COORDINATES = [25.7210773933098, -17.8090175647775]
PRIMARY_CONCEPT_LABELS = [
    "Note",
    "Community work",
    "HW-Coexistence",
    "Awareness creation",
]

# Cluey observation that carries an opaque ``images`` attribute
IMAGES_ENTITY_ID = "O3494596-nff20c1dac0817042"

# First Africa (CSV-ingest) observation
AFRICA_PRIMARY_ENTITY_ID = "O1234-6832068424866241484-18"
AFRICA_PROJECT_NAME = "Demo Africa Upload"
AFRICA_FILENAME = "POI-2025-demo.tsv"
AFRICA_ACTION_TAKEN = "Pruned"

AFRICA_ENTITY_IDS = [
    "O1234-6832068424866241484-18",
    "O1234-3006453294253435466-1500",
    "O1234-3006453294253435466-1250",
]


def login_response() -> dict:
    return {"authenticated": True, "username": "demo"}


def facets_response() -> dict:
    return json.loads((_ASSETS / "facets.json").read_text())


def _cluey_results() -> list[dict]:
    return json.loads((_ASSETS / "cluey_observations.json").read_text())["results"]


def _africa_results() -> list[dict]:
    return json.loads((_ASSETS / "africa_observations.json").read_text())["results"]


def _results_for_groups(groups: list[str]) -> list[dict]:
    results: list[dict] = []
    if CLUEY_GROUP in groups:
        results.extend(_cluey_results())
    if AFRICA_GROUP in groups:
        results.extend(_africa_results())
    return results


def observations_page(
    groups: list[str],
    start: int = 1,
    page_length: int = 200,
) -> dict:
    """Return one page of observations for the requested groups.

    ``start`` is 1-indexed, matching SensingClues Focus ``options.start``.
    """
    results = _results_for_groups(groups)
    offset = max(start - 1, 0)
    page = results[offset : offset + page_length]
    return {
        "total": len(results),
        "start": start,
        "page-length": page_length,
        "results": page,
    }


def observations_empty() -> dict:
    return {
        "total": 0,
        "start": 1,
        "page-length": 200,
        "results": [],
    }
