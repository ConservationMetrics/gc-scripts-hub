import shutil
import uuid

import psycopg2

from f.connectors.locusmap.locusmap import main


def test_script_e2e_zip(pg_database, tmp_path):
    fixture_path = "f/connectors/locusmap/tests/assets/"
    tmp_fixture_path = tmp_path / "Favorites.zip"

    # Copy fixtures to a temp location
    shutil.copy(fixture_path + "Favorites.zip", tmp_fixture_path)

    asset_storage = tmp_path / "datalake"

    main(
        pg_database,
        "my_locusmap_points",
        tmp_fixture_path,
        asset_storage,
    )

    # Attachments are saved to disk
    assert (
        asset_storage / "my_locusmap_points" / "p_2025-01-09_15-_20250109_151051.jpg"
    ).exists()

    # Check that the data was inserted into the database
    with psycopg2.connect(**pg_database) as conn:
        # Survey responses from Favorites are written to a SQL Table in expected format
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM my_locusmap_points")
            assert cursor.fetchone()[0] == 2

            cursor.execute("SELECT _id FROM my_locusmap_points")
            _id = cursor.fetchone()[0]
            assert uuid.UUID(_id, version=5)

            cursor.execute("SELECT g__type FROM my_locusmap_points LIMIT 1")
            assert cursor.fetchone()[0] == "Point"

            cursor.execute("SELECT g__coordinates FROM my_locusmap_points LIMIT 1")
            assert cursor.fetchone()[0] == "[-73.974922, 40.768731]"

            cursor.execute(
                "SELECT attachments FROM my_locusmap_points ORDER BY attachments LIMIT 1 OFFSET 1"
            )
            assert (
                cursor.fetchone()[0]
                == "p_2025-01-09_15-_20250109_154929.jpg, p_2025-01-09_15-_20250109_154918.m4a"
            )

    # Check that the temp files were cleaned up
    assert not (tmp_path / "Favorites.csv").exists()
    assert not (tmp_path / "Favorites-attachments").exists()


def test_script_e2e_csv(pg_database, tmp_path):
    fixture_path = "f/connectors/locusmap/tests/assets/"
    tmp_fixture_path = tmp_path / "My points.csv"

    # Copy fixtures to a temp location
    shutil.copy(fixture_path + "My points.csv", tmp_fixture_path)

    asset_storage = tmp_path / "datalake"

    main(
        pg_database,
        "my_locusmap_points",
        tmp_fixture_path,
        asset_storage,
    )

    # Check that the data was inserted into the database
    with psycopg2.connect(**pg_database) as conn:
        # Survey responses from Favorites are written to a SQL Table in expected format
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM my_locusmap_points")
            assert cursor.fetchone()[0] == 2

    # Check that the temp files were cleaned up
    assert not (tmp_path / "My points.csv").exists()
