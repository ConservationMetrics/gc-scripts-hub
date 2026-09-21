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
from pathlib import Path

import requests

from f.common_logic.db_operations import postgresql
from f.common_logic.geo_utils import is_valid_longitude_latitude
from f.common_logic.identifier_utils import camel_to_snake
from f.connectors.csv.csv_to_postgres import main as save_csv_to_postgres

_API = "https://api.gbif.org/v1/occurrence/download"
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
        final_archive = destination / f"{download_key}.zip"
        final_csv = destination / f"{download_key}.csv"
        os.replace(archive, final_archive)
        os.replace(csv_path, final_csv)
        logger.info("GBIF archive saved to %s", final_archive)
        logger.info("GBIF occurrence CSV saved to %s (%d records)", final_csv, record_count)

    provenance = {
        "download_key": download_key,
        "doi": metadata.get("doi"),
        "license": metadata.get("license"),
        "predicate": request.get("predicate") if isinstance(request, dict) else None,
        "record_count": record_count,
    }
    provenance_path = destination / f"{download_key}.json"
    provenance_path.write_text(
        json.dumps(provenance, indent=2), encoding="utf-8"
    )
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
                if len(fieldnames) != len(set(fieldnames)) or set(generated_fields).intersection(
                    fieldnames
                ):
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


def _coordinates(longitude: str | None, latitude: str | None) -> list[float] | None:
    try:
        longitude_value = float(longitude)  # type: ignore[arg-type]
        latitude_value = float(latitude)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not is_valid_longitude_latitude(longitude_value, latitude_value):
        return None
    return [longitude_value, latitude_value]
