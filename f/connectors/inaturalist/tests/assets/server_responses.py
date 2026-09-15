"""Mock iNaturalist API responses for testing.

Fixtures match the v2 field spec the production code requests.
"""

import json
from pathlib import Path

_ASSETS = Path(__file__).parent

PROJECT_ID = "13795"
PROJECT_SLUG = "lake-accotink-park"
PROJECT_TITLE = "Lake Accotink Park"
USERNAME = "lagin6489"

# First observation in the ascending-id fixture (has geometry + photos)
PRIMARY_OBSERVATION_ID = 7276418
PRIMARY_PHOTO_ID = 9408078
PRIMARY_PHOTO_FILENAME = f"{PRIMARY_PHOTO_ID}.jpg"
PRIMARY_GBIF_OCCURRENCE_ID = "1586109875"

# Hand-set on 22885102; no Lake Accotink observer publishes an ORCID.
SYNTHETIC_OBSERVER_ORCID = "https://orcid.org/0000-0000-0000-0000"

OBSCURED_TAXON_GEOPRIVACY_ID = 7289153
OBSCURED_USER_GEOPRIVACY_ID = 62639580
CAPTIVE_OBSERVATION_ID = 187178048
NULL_TAXON_OBSERVATION_ID = 26622141
SOUND_ONLY_OBSERVATION_ID = 92912381
SOUND_FILENAME = "298892.m4a"
NEEDS_ID_OBSERVATION_ID = 22885102
MULTI_PHOTO_OBSERVATION_ID = 7276475
EMPTY_DESCRIPTION_OBSERVATION_ID = 7288496
NULL_ACCURACY_OBSERVATION_ID = 7288932

OBSERVATION_COUNT = 10


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
