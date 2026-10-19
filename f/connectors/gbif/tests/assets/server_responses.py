"""Deterministic GBIF API response payloads used by connector tests."""

import json
from functools import lru_cache
from pathlib import Path

DOWNLOAD_KEY = "0002881-260916113435855"
ARCHIVE_URL = "https://downloads.example.test/gbif.zip"
DATASET_KEY = "4fa7b334-ce0d-4e88-aaae-2e0c138d049e"
PUBLISHING_ORG_KEY = "e2e717bf-551a-4917-bdc9-4fa0f342c530"


def metadata(status="SUCCEEDED"):
    """Return minimal public download metadata."""
    return {
        "key": DOWNLOAD_KEY,
        "status": status,
        "downloadLink": ARCHIVE_URL,
        "doi": "10.15468/dl.4wyr7y",
        "license": "http://creativecommons.org/licenses/by-nc/4.0/legalcode",
        "request": {"format": "SIMPLE_CSV", "predicate": {"type": "within"}},
    }


@lru_cache(maxsize=1)
def statistics_snapshot() -> dict:
    """Return the reduced facet excerpt for the example bounding box."""
    path = Path(__file__).with_name("gbif-statistics-20260930.json")
    return json.loads(path.read_text(encoding="utf-8"))
