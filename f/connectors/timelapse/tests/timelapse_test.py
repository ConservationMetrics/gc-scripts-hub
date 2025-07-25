import zipfile
from pathlib import Path

import pandas as pd
import psycopg2
import pytest

from f.connectors.timelapse.timelapse import _transform_df, main


def test_transform_df_column_name_collision():
    # Create a DataFrame with columns that will collide after transformation
    df = pd.DataFrame({"SomeColumn": [1, 2, 3], "Some_Column": [4, 5, 6]})

    with pytest.raises(ValueError, match="Column name collision detected"):
        _transform_df(df)


@pytest.fixture
def timelapse_zip(tmp_path):
    """
    Simulate the timelapse_export.zip file as input.

    Creates a .zip of everything under tests/assets/ and returns its path.
    We keep the assets uncompressed in the repo so they can be easily inspected
    without needing to unzip a binary archive every time.
    """
    zip_path = tmp_path / "timelapse_export.zip"
    source_dir = Path("f/connectors/timelapse/tests/assets/")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in source_dir.rglob("*"):
            if file.is_file():
                arcname = file.relative_to(source_dir)
                zipf.write(file, arcname)
    return zip_path


def test_script_e2e(pg_database, tmp_path, timelapse_zip):
    asset_storage = tmp_path / "datalake"

    actual_storage_path = main(
        timelapse_zip,
        pg_database,
        "my_timelapse_project",
        delete_timelapse_zip=True,
        attachment_root=asset_storage,
    )

    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM my_timelapse_project_data")
            assert cursor.fetchone()[0] == 4

            cursor.execute(
                "SELECT img_problem FROM my_timelapse_project_data WHERE _id = '2'"
            )
            assert cursor.fetchone()[0] == "malfunction"

            cursor.execute("SELECT COUNT(*) FROM my_timelapse_project_data_template")
            assert cursor.fetchone()[0] == 16

            cursor.execute(
                "SELECT data_label, tooltip FROM my_timelapse_project_data_template"
            )
            assert cursor.description[0].name == "data_label"
            assert cursor.description[1].name == "tooltip"

            cursor.execute(
                "SELECT project_org FROM my_timelapse_project_level_1 WHERE _id = '1'"
            )
            assert cursor.fetchone()[0] == "CMI"

            cursor.execute(
                "SELECT station_name FROM my_timelapse_project_level_2 WHERE _id = '4'"
            )
            assert cursor.fetchone()[0] == "Washington DC"

            cursor.execute(
                "SELECT deployment_trig_modes FROM my_timelapse_project_level_3 WHERE _id = '3'"
            )
            assert (
                cursor.fetchone()[0]
                == "Motion Image + Time_-lapse Image + Video,Time_-lapse Image,Time_-lapse Image + Video"
            )

            cursor.execute(
                "SELECT COUNT(*) FROM my_timelapse_project_folder_metadata_template"
            )
            assert cursor.fetchone()[0] == 20

    assert (actual_storage_path / "TimelapseData.ddb").exists()

    assert (actual_storage_path / "Station1" / "Deployment1a" / "IMG_002.jpg").exists()

    assert (actual_storage_path / "Station2" / "Deployment2a" / "IMG_001.jpg").exists()

    assert not (actual_storage_path / "Backups").exists()


def test_second_run_adds_suffix(pg_database, tmp_path, timelapse_zip):
    asset_storage = tmp_path / "datalake"

    # First run
    first_path = main(
        timelapse_zip,
        pg_database,
        "my_timelapse_project",
        delete_timelapse_zip=False,  # don't delete the zip, we need it again
        attachment_root=asset_storage,
    )

    # Second run
    second_path = main(
        timelapse_zip,
        pg_database,
        "my_timelapse_project",
        delete_timelapse_zip=False,
        attachment_root=asset_storage,
    )

    assert first_path.name == "timelapse_export"
    assert second_path.name == "timelapse_export_1"
    assert second_path.parent == first_path.parent
    assert (second_path / "TimelapseData.ddb").exists()
