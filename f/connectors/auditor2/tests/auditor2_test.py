import csv
import shutil
import zipfile
from pathlib import Path

import psycopg2
import pytest

from f.connectors.auditor2.auditor2 import main


@pytest.fixture
def auditor2_zip(tmp_path):
    """
    Creates a zip of test assets in a temporary location.

    Copies everything from the static assets directory, duplicates mock audio files,
    and returns a path to the resulting ZIP file.
    """
    original_assets = Path("f/connectors/auditor2/tests/assets")
    staging_dir = tmp_path / "assets"
    shutil.copytree(original_assets, staging_dir)

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
    zip_path = tmp_path / "auditor2_20250505.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in staging_dir.rglob("*"):
            if file.is_file():
                arcname = file.relative_to(staging_dir)
                zipf.write(file, arcname)

    return zip_path


def test_script_e2e(pg_database, tmp_path, auditor2_zip):
    asset_storage = tmp_path / "datalake"

    project_name = "my_auditor2_project"

    main(
        auditor2_zip,
        pg_database,
        project_name,
        delete_auditor2_zip=True,
        attachment_root=asset_storage,
    )

    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            # Basic row count checks for the imported tables
            cursor.execute("SELECT COUNT(*) FROM my_auditor2_project_deployments")
            assert cursor.fetchone()[0] == 56
            cursor.execute("SELECT COUNT(*) FROM my_auditor2_project_sites")
            assert cursor.fetchone()[0] == 20
            cursor.execute(
                "SELECT COUNT(*) FROM my_auditor2_project_human_readable_labels"
            )
            assert cursor.fetchone()[0] == 28
            cursor.execute(
                "SELECT COUNT(*) FROM my_auditor2_project_sound_file_summary"
            )
            assert cursor.fetchone()[0] == 56
            cursor.execute("SELECT COUNT(*) FROM my_auditor2_project_labels")
            assert cursor.fetchone()[0] == 95

            # Check that the sites table has g__coordinates and g__type fields
            cursor.execute(
                "SELECT g__coordinates, g__type FROM my_auditor2_project_sites LIMIT 1"
            )
            site_row = cursor.fetchone()
            assert len(site_row) == 2
            assert site_row[0] == "[-77.2264, 38.7881]"
            assert site_row[1] == "Point"

            # Check that the media files were copied correctly and match the database entries
            cursor.execute(
                f"SELECT filename, sound_path_wav, spectrogram_path "
                f"FROM {project_name}_labels "
                f"ORDER BY clip_id ASC LIMIT 3"
            )
            rows = cursor.fetchall()

            for row in rows:
                for rel_path in row:
                    full_path = (
                        asset_storage
                        / "Auditor2"
                        / project_name
                        / Path(auditor2_zip).stem
                        / rel_path
                    )
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
        csv_path = (
            asset_storage
            / "Auditor2"
            / project_name
            / Path(auditor2_zip).stem
            / csv_name
        )
        assert csv_path.exists(), f"Expected CSV not found: {csv_path}"

    # Check to see that the ZIP file was deleted
    assert not auditor2_zip.exists(), "Auditor2 ZIP file was not deleted as expected."


def test_missing_csv_raises_error(pg_database, tmp_path, auditor2_zip):
    asset_storage = tmp_path / "datalake"
    project_name = "my_auditor2_project"

    # Extract original zip to a temp dir
    extracted_dir = tmp_path / "extracted_assets"
    with zipfile.ZipFile(auditor2_zip, "r") as zip_ref:
        zip_ref.extractall(extracted_dir)

    # Remove the 'labels' CSV file
    (extracted_dir / "lake_accotink_labels_20250505.csv").unlink()

    # Recreate the zip without the missing CSV
    incomplete_zip_path = tmp_path / "incomplete_auditor2_20250505.zip"
    with zipfile.ZipFile(incomplete_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in extracted_dir.rglob("*"):
            if file.is_file():
                arcname = file.relative_to(extracted_dir)
                zipf.write(file, arcname)

    # Run the main function and expect a ValueError
    with pytest.raises(ValueError, match="Missing required CSV"):
        main(
            incomplete_zip_path,
            pg_database,
            project_name,
            delete_auditor2_zip=True,
            attachment_root=asset_storage,
        )
