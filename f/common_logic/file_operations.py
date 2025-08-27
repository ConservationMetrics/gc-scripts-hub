import base64
import csv
import json
import logging
import tempfile
import zipfile
from io import StringIO
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_csv_to_list(csv_path):
    """
    Read CSV file and return as list of dictionaries.

    Parameters
    ----------
    csv_path : str or pathlib.Path
        Path to the CSV file to read.

    Returns
    -------
    list of dict
        List of dictionaries representing CSV rows with column headers as keys.
    """
    logger.info(f"Reading CSV file: {csv_path}")
    with open(csv_path, "r", encoding="utf-8") as f:
        data = list(csv.DictReader(f))
    logger.info(f"Read {len(data)} rows from CSV file")
    return data


def list_to_csv_string(data):
    """
    Convert list of dictionaries to CSV string.

    Parameters
    ----------
    data : list of dict
        List of dictionaries to convert to CSV format.

    Returns
    -------
    str
        CSV-formatted string with headers and data rows.
        Returns empty string if input data is empty.
    """
    if not data:
        return ""

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()


def get_safe_file_path(storage_path: str, db_table_name: str, file_type: str):
    """
    Construct a safe file path for storing data, ensuring it remains within the specified storage directory;
    otherwise, raises a ValueError.
    """
    storage_path = Path(storage_path).resolve()
    file_path = (storage_path / f"{db_table_name}.{file_type}").resolve()

    if not file_path.is_relative_to(storage_path):
        raise ValueError("Invalid path: possible path traversal detected.")

    return file_path


def save_data_to_file(data, filename: str, storage_path: str, file_type: str = "json"):
    """
    Saves the provided data to a file in the specified format and storage path.

    Parameters
    ----------
    data : list or dict
        The data to be saved. For CSV, should be a list of rows including a header.
    filename : str
        The name of the file to save the data to, without extension.
    storage_path : str
        The directory path where the file will be saved.
    file_type : str
        The format to save the file as: "json", "geojson", or "csv".
    """
    storage_path = Path(storage_path)
    storage_path.mkdir(parents=True, exist_ok=True)

    if not data or (
        # Extra check for geojson files to avoid saving empty files
        file_type in {"geojson"} and isinstance(data, dict) and not data.get("features")
    ):
        logger.warning(f"No data to save for file: {filename}.{file_type}")
        return

    file_path = get_safe_file_path(storage_path, filename, file_type)

    if file_type in {"geojson", "json"}:
        with file_path.open("w") as f:
            json.dump(data, f)
    elif file_type == "csv":
        with file_path.open("w", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerows(data)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    logger.info(f"{file_type.upper()} file saved to {file_path}")


def save_uploaded_file_to_temp(
    uploaded_file, tmp_dir: str = "/persistent-storage/tmp", is_base64: bool = True
):
    """
    Saves an uploaded file to a temp directory and extracts it if it's a ZIP or KMZ.

    Notes:

    * We accept ZIP files to support uploading data with attachments,
    or in the future, data types that consist of multiple files like
    ESRI shapefiles.
    * The expectation is that a downstream process will delete
    the temporary files in `tmp_dir` after processing.

    Parameters
    ----------
    uploaded_file : list of dict
        List with one dict: {"name": str, "data": str or bytes}
        This is the currently used format in which files are uploaded to a
        Windmill app. See https://www.windmill.dev/docs/core_concepts/files_binary_data
    tmp_dir : str
        Directory to save file(s). Default: /persistent-storage/tmp
    is_base64 : bool
        Whether the data is base64 encoded. Default: True

    Returns
    -------
        dict
            On success:
                - file_paths: list of one or more saved file paths (extracted if zip)
            On failure:
                - error: string error message
    """
    try:
        file_info = uploaded_file[0]
        filename = file_info["name"]
        data = file_info["data"]

        # Decode base64 if specified, otherwise treat as raw data
        if is_base64:
            raw_data = base64.b64decode(data.encode())
        else:
            # If not base64 encoded, treat as raw bytes
            raw_data = data.encode() if isinstance(data, str) else data

        tmp_dir_path = Path(tmp_dir)
        tmp_dir_path.mkdir(parents=True, exist_ok=True)

        file_path = tmp_dir_path / filename
        with open(file_path, "wb") as f:
            f.write(raw_data)

        logger.debug(f"Saved file to temp: {file_path}")

        is_kmz = file_path.suffix.lower() == ".kmz"

        # Don't extract Excel files even though they are ZIP archives
        # (In the future, we could add other archive file types to this list
        # such as Word, PowerPoint, etc.)
        is_zip = zipfile.is_zipfile(file_path) and file_path.suffix.lower() not in [
            ".xlsx",
            ".xls",
        ]

        if is_kmz or is_zip:
            extract_dir = file_path.with_suffix("")

            # A KMZ file is essentially a ZIP archive
            # https://developers.google.com/kml/documentation/kmzarchives
            archive_path = file_path
            if is_kmz:
                with tempfile.NamedTemporaryFile(
                    suffix=".zip", delete=False
                ) as tmp_zip:
                    tmp_zip.write(file_path.read_bytes())
                    archive_path = Path(tmp_zip.name)

            with zipfile.ZipFile(archive_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)
                file_paths = [str(p) for p in extract_dir.rglob("*") if p.is_file()]
                logger.debug(f"Extracted {len(file_paths)} files to {extract_dir}")

            if is_kmz:
                archive_path.unlink()  # clean up temp .zip

            file_path.unlink()  # remove original .kmz or .zip

        else:
            file_paths = [str(file_path)]
        return {"file_paths": file_paths}

    except Exception as e:
        logger.error(f"Error processing uploaded file: {e}")
        return {"error": str(e)}
