import csv
import shutil
import zipfile
from pathlib import Path
from unittest.mock import patch

import psycopg
import pytest

from f.connectors.auditor2.auditor2 import main, read_auditor2_csvs


def test_read_auditor2_csvs_basic(tmp_path):
    # Setup fake CSVs with expected keys
    keys = [
        "deployments",
        "human_readable_labels",
        "labels",
        "sites",
        "sound_file_summary",
    ]

    for key in keys:
        file_path = tmp_path / f"project_{key}_20250505.csv"
        file_path.write_text("col1,col2\nval1,val2", encoding="utf-8")

    result = read_auditor2_csvs(tmp_path)

    assert set(result.keys()) == set(keys)
    for table in result.values():
        assert isinstance(table, list)
        assert len(table) == 1
        assert table[0]["col1"] == "val1"


def test_read_auditor2_csvs_raises_on_duplicate(tmp_path):
    # Create two files that both match "labels"
    (tmp_path / "project_labels_20250505.csv").write_text(
        "col1,col2\nval1,val2", encoding="utf-8"
    )
    (tmp_path / "another_labels_file.csv").write_text(
        "col1,col2\nval3,val4", encoding="utf-8"
    )

    # Also create the other required CSVs
    for key in ["deployments", "human_readable_labels", "sites", "sound_file_summary"]:
        (tmp_path / f"dummy_{key}_file.csv").write_text(
            "col1,col2\nval1,val2", encoding="utf-8"
        )

    with pytest.raises(ValueError, match="Multiple CSV files found matching 'labels'"):
        read_auditor2_csvs(tmp_path)


def _prepare_auditor2_assets(tmp_path, with_media: bool):
    """
    Creates a zip of test assets in a temporary location.

    Copies everything from the static assets directory. If `with_media` is True,
    it also duplicates mock audio files referenced in the CSV.
    (This simulates the presence of the expected media files for testing.)

    Returns a path to the resulting ZIP file.
    """
    original_assets = Path("f/connectors/auditor2/tests/assets")
    staging_dir = tmp_path / "assets"
    shutil.copytree(original_assets, staging_dir)

    if with_media:
        data_dir = staging_dir / "Lake_Accotink_2023_R1_FLAC"
        csv_path = staging_dir / "lake_accotink_labels_20250505.csv"

        # Mock source files to copy
        source_files = {
            "flac": data_dir / "audio.flac",
            "wav": data_dir / "audio.wav",
            "jpg": data_dir / "spectogram.jpg",
        }

        # Replicate fake files for every row in the CSV
        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for field, ext in [
                    ("filename", ".flac"),
                    ("sound_path_wav", ".wav"),
                    ("spectrogram_path", ".jpg"),
                ]:
                    rel_path = Path(row[field])
                    dest_path = staging_dir / rel_path
                    if not dest_path.exists():
                        dest_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(source_files[ext.lstrip(".")], dest_path)

    # Now zip it all up
    zip_path = tmp_path / f"auditor2_20250505{'_with_media' if with_media else ''}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in staging_dir.rglob("*"):
            if file.is_file():
                arcname = file.relative_to(staging_dir)
                zipf.write(file, arcname)

    return zip_path


@pytest.fixture
def auditor2_zip_with_media(tmp_path):
    return _prepare_auditor2_assets(tmp_path, with_media=True)


@pytest.fixture
def auditor2_zip_without_media(tmp_path):
    return _prepare_auditor2_assets(tmp_path, with_media=False)


project_name = "lake_accotink_biacoustics"

# Create mock azure_blob resource
azure_blob = {
    "accountName": "testaccount",
    "containerName": "test_container",
    "accessKey": "testkey",
    "useSSL": True,
    "endpoint": "core.windows.net",
}


def test_script_e2e(pg_database, tmp_path, auditor2_zip_with_media):
    asset_storage = tmp_path / "datalake"

    # Mock the Azure Blob Storage download to return our test zip file
    with patch("f.connectors.auditor2.auditor2.download_blob_to_temp") as mock_download:
        mock_download.return_value = auditor2_zip_with_media

        actual_storage_path = main(
            azure_blob=azure_blob,
            blob_name="auditor2_20250505_with_media.zip",
            db=pg_database,
            project_name=project_name,
            attachment_root=asset_storage,
        )

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            # Basic row count checks for the imported tables
            cursor.execute(f"SELECT COUNT(*) FROM auditor2_{project_name}_deployments")
            assert cursor.fetchone()[0] == 56
            cursor.execute(f"SELECT COUNT(*) FROM auditor2_{project_name}_sites")
            assert cursor.fetchone()[0] == 20
            cursor.execute(
                f"SELECT COUNT(*) FROM auditor2_{project_name}_human_readable_labels"
            )
            assert cursor.fetchone()[0] == 28
            cursor.execute(
                f"SELECT COUNT(*) FROM auditor2_{project_name}_sound_file_summary"
            )
            assert cursor.fetchone()[0] == 56
            cursor.execute(f"SELECT COUNT(*) FROM auditor2_{project_name}_labels")
            assert cursor.fetchone()[0] == 95

            # Check that the sites table has g__coordinates and g__type fields
            cursor.execute(
                f"SELECT g__coordinates, g__type FROM auditor2_{project_name}_sites LIMIT 1"
            )
            site_row = cursor.fetchone()
            assert len(site_row) == 2
            assert site_row[0] == "[-77.2264, 38.7881]"
            assert site_row[1] == "Point"

            # Check that the media files were copied correctly and match the database entries
            cursor.execute(
                f"SELECT filename, sound_path_wav, spectrogram_path "
                f"FROM auditor2_{project_name}_labels "
                f"ORDER BY clip_id ASC LIMIT 3"
            )
            rows = cursor.fetchall()

            for row in rows:
                for rel_path in row:
                    full_path = asset_storage / "Auditor2" / project_name / rel_path
                    assert full_path.exists(), f"Missing file: {full_path}"

    # Check that the CSVs were copied to the expected location
    expected_csvs = [
        "lake_accotink_deployments_20250505.csv",
        "lake_accotink_human_readable_labels_20250505.csv",
        "lake_accotink_labels_20250505.csv",
        "lake_accotink_sites_20250505.csv",
        "lake_accotink_sound_file_summary_20250505.csv",
    ]
    for csv_name in expected_csvs:
        csv_path = asset_storage / "Auditor2" / project_name / csv_name
        assert csv_path.exists(), f"Expected CSV not found: {csv_path}"

    # Check that actual_storage_path is correctly returned
    expected_storage_path = asset_storage / "Auditor2" / project_name
    assert actual_storage_path == expected_storage_path


def test_raise_if_project_name_exists(
    pg_database, tmp_path, auditor2_zip_without_media
):
    asset_storage = tmp_path / "datalake"

    # Mock the Azure Blob Storage download to return our test zip file
    with patch("f.connectors.auditor2.auditor2.download_blob_to_temp") as mock_download:
        mock_download.return_value = auditor2_zip_without_media

        # Run the main function to create the tables once
        main(
            azure_blob=azure_blob,
            blob_name="auditor2_20250505.zip",
            db=pg_database,
            project_name=project_name,
            attachment_root=asset_storage,
        )

        # Try to run again with the same project name - should raise an error
        with pytest.raises(ValueError, match="Auditor2 project name already in usage"):
            main(
                azure_blob=azure_blob,
                blob_name="auditor2_20250505.zip",
                db=pg_database,
                project_name=project_name,
                attachment_root=asset_storage,
            )


def test_zip_file_not_found(pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"

    # Mock the Azure Blob Storage download to raise an exception
    with patch("f.connectors.auditor2.auditor2.download_blob_to_temp") as mock_download:
        mock_download.side_effect = Exception("Blob not found")

        with pytest.raises(Exception, match="Blob not found"):
            main(
                azure_blob=azure_blob,
                blob_name="nonexistent.zip",
                db=pg_database,
                project_name="test_project",
                attachment_root=asset_storage,
            )


def test_missing_csv_raises_error(pg_database, tmp_path, auditor2_zip_without_media):
    asset_storage = tmp_path / "datalake"

    # Create a ZIP file missing one of the required CSVs
    staging_dir = tmp_path / "incomplete_assets"
    staging_dir.mkdir()

    # Create only 4 of the 5 required CSVs
    incomplete_keys = [
        "deployments",
        "human_readable_labels",
        "labels",
        "sites",
        # Missing: sound_file_summary
    ]

    for key in incomplete_keys:
        file_path = staging_dir / f"project_{key}_20250505.csv"
        file_path.write_text("col1,col2\nval1,val2", encoding="utf-8")

    # Create ZIP with incomplete CSV set
    incomplete_zip = tmp_path / "incomplete.zip"
    with zipfile.ZipFile(incomplete_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in staging_dir.rglob("*"):
            if file.is_file():
                arcname = file.relative_to(staging_dir)
                zipf.write(file, arcname)

    # Mock the Azure Blob Storage download to return the incomplete zip
    with patch("f.connectors.auditor2.auditor2.download_blob_to_temp") as mock_download:
        mock_download.return_value = incomplete_zip

        with pytest.raises(ValueError, match="Missing required CSV file"):
            main(
                azure_blob=azure_blob,
                blob_name="incomplete.zip",
                db=pg_database,
                project_name="test_project",
                attachment_root=asset_storage,
            )
