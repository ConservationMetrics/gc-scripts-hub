"""Deterministic GBIF API response payloads used by connector tests."""

DOWNLOAD_KEY = "0002881-260916113435855"
ARCHIVE_URL = "https://downloads.example.test/gbif.zip"


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
