"""Mock iNaturalist API responses for testing.

Fixtures are trimmed to the fields the production code reads.
"""

import json
from pathlib import Path

_ASSETS = Path(__file__).parent

PROJECT_ID = "13795"
PROJECT_SLUG = "lake-accotink-park"
PROJECT_TITLE = "Lake Accotink Park"

# First observation in the ascending-id fixture (has geometry + photo)
PRIMARY_OBSERVATION_ID = 7276418


def _load_observations() -> dict:
    return json.loads((_ASSETS / "observations_fixture.json").read_text())


def project_metadata() -> dict:
    return json.loads((_ASSETS / "project_fixture.json").read_text())


def observations_page(id_above: int | None = None, per_page: int = 200) -> dict:
    """Return a single page of fixture observations, optionally filtered by id_above."""
    data = _load_observations()
    results = data["results"]
    if id_above is not None:
        results = [o for o in results if o["id"] > id_above]
    page = results[:per_page]
    return {
        "total_results": data.get("total_results", len(results)),
        "page": 1,
        "per_page": per_page,
        "results": page,
    }


def observations_paginated(id_above: int | None = None, per_page: int = 2) -> dict:
    """Split the fixture into pages of ``per_page`` for pagination tests."""
    return observations_page(id_above=id_above, per_page=per_page)


def observations_empty() -> dict:
    return {
        "total_results": 0,
        "page": 1,
        "per_page": 200,
        "results": [],
    }
