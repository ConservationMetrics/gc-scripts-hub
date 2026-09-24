# requirements:
# psycopg[binary]
# requests

"""Download, validate, preserve, and import a completed GBIF SIMPLE_CSV archive."""

import csv
import json
import logging
import os
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor
from itertools import zip_longest
from pathlib import Path
from time import monotonic
from urllib.parse import quote

import requests

from f.common_logic.db_operations import postgresql
from f.common_logic.geo_utils import is_valid_longitude_latitude
from f.common_logic.identifier_utils import camel_to_snake
from f.connectors.csv.csv_to_postgres import main as save_csv_to_postgres

_API = "https://api.gbif.org/v1/occurrence/download"
_REGISTRY_API = "https://api.gbif.org/v1"
_REGISTRY_DEADLINE_SECONDS = 120
_REGISTRY_TIMEOUT = (5, 20)
_REGISTRY_WORKERS = 8
_REGISTRY_USER_AGENT = (
    "GuardianConnector GBIF connector "
    "(https://github.com/ConservationMetrics/gc-scripts-hub)"
)
logger = logging.getLogger(__name__)


def main(
    download_key: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
) -> dict:
    """Fetch one successful GBIF archive and import its TSV occurrences as CSV.

    Parameters
    ----------
    download_key : str
        Completed GBIF download key.
    db : postgresql
        Database connection resource.
    db_table_name : str
        Destination table and datalake subdirectory name.
    attachment_root : str
        Root directory for retained archives, CSV, and provenance metadata.
    """
    if (
        not db_table_name
        or len(db_table_name) > 54
        or "/" in db_table_name
        or "\\" in db_table_name
    ):
        raise ValueError(
            "db_table_name must be a non-empty table name of at most 54 characters."
        )
    metadata = _metadata(download_key)
    if metadata.get("status") != "SUCCEEDED":
        raise RuntimeError(f"GBIF download {download_key} is not SUCCEEDED.")
    request = metadata.get("request")
    if not isinstance(request, dict) or request.get("format") != "SIMPLE_CSV":
        raise RuntimeError(f"GBIF download {download_key} is not SIMPLE_CSV.")
    download_url = metadata.get("downloadLink") or metadata.get("downloadUrl")
    if not isinstance(download_url, str) or not download_url:
        raise RuntimeError(f"GBIF download {download_key} has no archive URL.")

    destination = Path(attachment_root) / db_table_name
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=destination) as temporary_directory:
        temporary = Path(temporary_directory)
        archive = temporary / "download.zip"
        _stream_download(download_url, archive)
        csv_path, record_count = _convert_archive(
            archive, temporary / "occurrences.csv"
        )
        enriched_csv = temporary / "enriched-occurrences.csv"
        enrichment = _enrich_csv(csv_path, enriched_csv)
        csv_path.unlink()
        csv_path = enriched_csv
        final_archive = destination / f"{download_key}.zip"
        final_csv = destination / f"{download_key}.csv"
        os.replace(archive, final_archive)
        os.replace(csv_path, final_csv)
        logger.info("GBIF archive saved to %s", final_archive)
        logger.info(
            "GBIF occurrence CSV saved to %s (%d records)", final_csv, record_count
        )

    provenance = {
        "download_key": download_key,
        "doi": metadata.get("doi"),
        "license": metadata.get("license"),
        "predicate": request.get("predicate") if isinstance(request, dict) else None,
        "record_count": record_count,
        "enrichment": enrichment,
    }
    provenance_path = destination / f"{download_key}.json"
    provenance_path.write_text(json.dumps(provenance, indent=2), encoding="utf-8")
    logger.info("GBIF provenance metadata saved to %s", provenance_path)
    if record_count:
        save_csv_to_postgres(
            db,
            db_table_name,
            str(final_csv.relative_to(attachment_root)),
            attachment_root,
            delete_csv_file=False,
            id_column="_id",
        )
    return {
        "download_key": download_key,
        "doi": metadata.get("doi"),
        "destination": db_table_name,
        "record_count": record_count,
    }


def _metadata(download_key: str) -> dict:
    response = requests.get(f"{_API}/{download_key}", timeout=(10, 60))
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise TypeError("GBIF returned invalid download metadata.")
    return payload


def _stream_download(url: str, destination: Path) -> None:
    with requests.get(url, stream=True, timeout=(10, 300)) as response:
        response.raise_for_status()
        with destination.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)


def _convert_archive(archive: Path, output_path: Path) -> tuple[Path, int]:
    try:
        with zipfile.ZipFile(archive) as zipped:
            members = [
                member
                for member in zipped.infolist()
                if member.filename.lower().endswith(".csv") and not member.is_dir()
            ]
            if len(members) != 1:
                raise ValueError(
                    "GBIF archive must contain exactly one occurrence CSV member."
                )
            with (
                zipped.open(members[0]) as source,
                output_path.open("w", encoding="utf-8", newline="") as output,
            ):
                reader = csv.DictReader(
                    (line.decode("utf-8") for line in source),
                    delimiter="\t",
                    quoting=csv.QUOTE_NONE,
                )
                if not reader.fieldnames or len(reader.fieldnames) < 2:
                    raise ValueError("GBIF occurrence TSV has no usable header.")
                fieldnames = [camel_to_snake(name) for name in reader.fieldnames]
                generated_fields = ("_id", "g__type", "g__coordinates")
                reserved_fields = (*generated_fields, "dataset", "publishing_org")
                if len(fieldnames) != len(set(fieldnames)) or set(
                    reserved_fields
                ).intersection(fieldnames):
                    raise ValueError(
                        "GBIF occurrence TSV has headers that conflict after snake_case conversion."
                    )
                fieldnames.extend(generated_fields)
                writer = csv.DictWriter(output, fieldnames=fieldnames)
                writer.writeheader()
                seen_ids: set[str] = set()
                count = 0
                for row in reader:
                    if None in row:
                        raise ValueError("GBIF occurrence TSV has malformed rows.")
                    row = {camel_to_snake(name): value for name, value in row.items()}
                    gbif_id = (row.get("gbif_id") or "").strip()
                    if not gbif_id:
                        raise ValueError("GBIF occurrence TSV has a missing gbif_id.")
                    if gbif_id in seen_ids:
                        raise ValueError(
                            f"GBIF occurrence TSV has duplicate gbif_id {gbif_id}."
                        )
                    seen_ids.add(gbif_id)
                    row["_id"] = gbif_id
                    coordinates = _coordinates(
                        row.get("decimal_longitude"), row.get("decimal_latitude")
                    )
                    row["g__type"] = "Point" if coordinates else ""
                    row["g__coordinates"] = (
                        json.dumps(coordinates) if coordinates else ""
                    )
                    writer.writerow(row)
                    count += 1
    except (OSError, UnicodeDecodeError, zipfile.BadZipFile) as exc:
        raise ValueError("GBIF archive is corrupt or not UTF-8 TSV data.") from exc
    return output_path, count


def _enrich_csv(source_path: Path, output_path: Path) -> dict[str, int]:
    """Add GBIF dataset and publishing organization titles to a converted CSV."""
    with source_path.open(encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError("Converted GBIF CSV has no header.")
        dataset_keys: set[str] = set()
        publishing_org_keys: set[str] = set()
        for row in reader:
            dataset_key = (row.get("dataset_key") or "").strip()
            publishing_org_key = (row.get("publishing_org_key") or "").strip()
            if dataset_key:
                dataset_keys.add(dataset_key)
            if publishing_org_key:
                publishing_org_keys.add(publishing_org_key)

    dataset_titles, publishing_org_titles = _resolve_registry_titles(
        dataset_keys, publishing_org_keys
    )

    with (
        source_path.open(encoding="utf-8", newline="") as source,
        output_path.open("w", encoding="utf-8", newline="") as output,
    ):
        reader = csv.DictReader(source)
        fieldnames = [*(reader.fieldnames or []), "dataset", "publishing_org"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            dataset_key = (row.get("dataset_key") or "").strip()
            publishing_org_key = (row.get("publishing_org_key") or "").strip()
            row["dataset"] = dataset_titles.get(dataset_key, "")
            row["publishing_org"] = publishing_org_titles.get(publishing_org_key, "")
            writer.writerow(row)

    return {
        "dataset_keys": len(dataset_keys),
        "datasets_resolved": len(dataset_titles),
        "publishing_org_keys": len(publishing_org_keys),
        "publishing_orgs_resolved": len(publishing_org_titles),
    }


def _resolve_registry_titles(
    dataset_keys: set[str], publishing_org_keys: set[str]
) -> tuple[dict[str, str], dict[str, str]]:
    """Resolve distinct GBIF Registry keys with bounded request concurrency."""
    lookups: list[tuple[str, str]] = []
    for dataset_key, publishing_org_key in zip_longest(
        sorted(dataset_keys), sorted(publishing_org_keys)
    ):
        if dataset_key is not None:
            lookups.append(("dataset", dataset_key))
        if publishing_org_key is not None:
            lookups.append(("organization", publishing_org_key))
    if not lookups:
        return {}, {}

    dataset_titles: dict[str, str] = {}
    publishing_org_titles: dict[str, str] = {}
    deadline = monotonic() + _REGISTRY_DEADLINE_SECONDS
    for offset in range(0, len(lookups), _REGISTRY_WORKERS):
        if monotonic() >= deadline:
            logger.warning(
                "GBIF Registry enrichment reached its %d-second deadline.",
                _REGISTRY_DEADLINE_SECONDS,
            )
            break
        batch = lookups[offset : offset + _REGISTRY_WORKERS]
        with ThreadPoolExecutor(max_workers=len(batch)) as executor:
            results = executor.map(lambda lookup: _registry_title(*lookup), batch)
            for (resource_type, key), title in zip(batch, results, strict=True):
                if title is None:
                    continue
                if resource_type == "dataset":
                    dataset_titles[key] = title
                else:
                    publishing_org_titles[key] = title

    unresolved_count = len(lookups) - len(dataset_titles) - len(publishing_org_titles)
    if unresolved_count:
        logger.warning(
            "GBIF Registry enrichment left %d of %d keys unresolved.",
            unresolved_count,
            len(lookups),
        )
    return dataset_titles, publishing_org_titles


def _registry_title(resource_type: str, key: str) -> str | None:
    """Return one Registry title, or ``None`` when lookup is unsuccessful."""
    url = f"{_REGISTRY_API}/{resource_type}/{quote(key, safe='')}"
    try:
        response = requests.get(
            url,
            headers={"User-Agent": _REGISTRY_USER_AGENT},
            timeout=_REGISTRY_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        logger.info("Could not resolve GBIF %s %s: %s", resource_type, key, exc)
        return None

    if not isinstance(payload, dict):
        logger.info("GBIF %s %s returned an invalid payload.", resource_type, key)
        return None
    title = payload.get("title")
    if not isinstance(title, str) or not title.strip():
        logger.info("GBIF %s %s returned no title.", resource_type, key)
        return None
    return title.strip()


def _coordinates(longitude: str | None, latitude: str | None) -> list[float] | None:
    try:
        longitude_value = float(longitude)  # type: ignore[arg-type]
        latitude_value = float(latitude)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not is_valid_longitude_latitude(longitude_value, latitude_value):
        return None
    return [longitude_value, latitude_value]
