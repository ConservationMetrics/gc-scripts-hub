import zipfile
from pathlib import Path

import psycopg2

from f.connectors.timelapse.timelapse import main


def test_script_e2e(pg_database, tmp_path):
    tmp_fixture_path = tmp_path / "timelapse.zip"
    source_dir = Path("f/connectors/timelapse/tests/assets/")

    # Zip up the contents of f/connectors/locusmap/tests/assets/ into tmp_fixture_path
    # to simulate the timelapse.zip file as input.
    # We keep the assets uncompressed in the repo so they can be easily inspected
    # without needing to unzip a binary archive every time.
    with zipfile.ZipFile(tmp_fixture_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in source_dir.rglob("*"):
            if file.is_file():
                arcname = file.relative_to(source_dir)
                zipf.write(file, arcname)

    asset_storage = tmp_path / "datalake"

    main(
        tmp_fixture_path,
        pg_database,
        "timelapse_test",
        delete_timelapse_zip=True,
        attachment_root=asset_storage,
    )

    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM timelapse_test_data")
            assert cursor.fetchone()[0] == 4

            cursor.execute(
                "SELECT img_problem FROM timelapse_test_data WHERE _id = '2'"
            )
            assert cursor.fetchone()[0] == "malfunction"

            cursor.execute("SELECT COUNT(*) FROM timelapse_test_data_template")
            assert cursor.fetchone()[0] == 16

            cursor.execute(
                "SELECT data_label, tooltip FROM timelapse_test_data_template"
            )
            assert cursor.description[0].name == "data_label"
            assert cursor.description[1].name == "tooltip"

            cursor.execute(
                "SELECT project_org FROM timelapse_test_level_1 WHERE _id = '1'"
            )
            assert cursor.fetchone()[0] == "CMI"

            cursor.execute(
                "SELECT station_name FROM timelapse_test_level_2 WHERE _id = '4'"
            )
            assert cursor.fetchone()[0] == "Washington DC"

            cursor.execute(
                "SELECT deployment_trig_modes FROM timelapse_test_level_3 WHERE _id = '3'"
            )
            assert (
                cursor.fetchone()[0]
                == "Motion Image + Time_-lapse Image + Video,Time_-lapse Image,Time_-lapse Image + Video"
            )

            cursor.execute(
                "SELECT COUNT(*) FROM timelapse_test_folder_metadata_template"
            )
            assert cursor.fetchone()[0] == 20

    assert (
        asset_storage / "Timelapse" / "timelapse_test" / "TimelapseData.ddb"
    ).exists()

    assert (
        asset_storage
        / "Timelapse"
        / "timelapse_test"
        / "Station1"
        / "Deployment1a"
        / "IMG_002.jpg"
    ).exists()

    assert (
        asset_storage
        / "Timelapse"
        / "timelapse_test"
        / "Station2"
        / "Deployment2a"
        / "IMG_001.jpg"
    ).exists()

    assert not (asset_storage / "Timelapse" / "timelapse_test" / "Backups").exists()
