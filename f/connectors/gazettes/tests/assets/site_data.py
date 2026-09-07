"""Shared test constants and asset loaders for the gazettes test suite."""

from pathlib import Path

ASSETS = Path("f/connectors/gazettes/tests/assets")

SITE_URL = "https://ggmc.test/gazettes/"
PDF_BASE = "https://ggmc.test/ggmc_web/wp-content/uploads/2026/07"
PDF_URL_JULY = f"{PDF_BASE}/Mines-4th-July-2026.pdf"
PDF_URL_FEB = f"{PDF_BASE}/Mines-21st-Feb-2026.pdf"
PDF_URL_ORDER = f"{PDF_BASE}/Order-No.-1-of-2026.pdf"
DOC_URL = "https://ggmc.test/ggmc_web/wp-content/uploads/2026/07/Mines-4th-July-2026.pdf"
PDF_URL_RELATIVE = (
    "https://ggmc.test/ggmc_web/wp-content/uploads/2025/12/Mines-27th-September-2025.pdf"
)

def read_asset(name):
    return (ASSETS / name).read_text(encoding="utf-8")


def read_asset_bytes(name):
    return (ASSETS / name).read_bytes()
