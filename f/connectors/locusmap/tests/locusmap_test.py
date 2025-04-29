import shutil
import uuid

import psycopg2
import pytest

from f.connectors.locusmap.locusmap import main

points_fixture_path = "f/connectors/locusmap/tests/assets/points/"


@pytest.mark.parametrize("file_format", ["csv", "gpx", "kml"])
def test_script_e2e_points(pg_database, tmp_path, file_format):
    tmp_fixture_path = tmp_path / f"Favorites.{file_format}"

    # Copy fixture to a temp location
    shutil.copy(points_fixture_path + f"Favorites.{file_format}", tmp_fixture_path)

    asset_storage = tmp_path / "datalake"

    main(
        pg_database,
        "my_locusmap_points",
        tmp_fixture_path,
        asset_storage,
    )

    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            # There are two rows
            cursor.execute("SELECT COUNT(*) FROM my_locusmap_points")
            assert cursor.fetchone()[0] == 2

            # The _id is a UUID
            cursor.execute("SELECT _id FROM my_locusmap_points")
            _id = cursor.fetchone()[0]
            assert uuid.UUID(_id, version=5)

            # The g__type is "Point"
            cursor.execute("SELECT g__type FROM my_locusmap_points LIMIT 1")
            assert cursor.fetchone()[0] == "Point"

            # The g__coordinates are in [lon, lat] format
            cursor.execute("SELECT g__coordinates FROM my_locusmap_points LIMIT 1")
            assert cursor.fetchone()[0] == "[-73.974922, 40.768731]"

            # The attachments are stored as a comma-separated string
            cursor.execute(
                "SELECT attachments FROM my_locusmap_points ORDER BY attachments LIMIT 1 OFFSET 1"
            )
            assert (
                cursor.fetchone()[0]
                == "p_2025-01-09_15-_20250109_154929.jpg, p_2025-01-09_15-_20250109_154918.m4a"
            )

    assert not tmp_fixture_path.exists()

    assert (
        asset_storage / "my_locusmap_points" / f"my_locusmap_points.{file_format}"
    ).exists()
    assert (
        asset_storage / "my_locusmap_points" / "my_locusmap_points.geojson"
    ).exists()


@pytest.mark.parametrize(
    "file_format,expected_extracted_filename",
    [
        ("zip", "my_locusmap_points.csv"),
        ("kmz", "my_locusmap_points.kml"),
    ],
)
def test_script_e2e_points_archive(
    pg_database, tmp_path, file_format, expected_extracted_filename
):
    tmp_fixture_path = tmp_path / f"Favorites.{file_format}"

    # Copy fixtures to a temp location
    shutil.copy(points_fixture_path + f"Favorites.{file_format}", tmp_fixture_path)

    asset_storage = tmp_path / "datalake"

    main(
        pg_database,
        "my_locusmap_points",
        tmp_fixture_path,
        asset_storage,
    )

    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM my_locusmap_points")
            assert cursor.fetchone()[0] == 2

    assert (
        asset_storage / "my_locusmap_points" / "p_2025-01-09_15-_20250109_151051.jpg"
    ).exists()
    assert (
        asset_storage / "my_locusmap_points" / "p_2025-01-09_15-_20250109_154918.m4a"
    ).exists()

    assert not (tmp_path / "Favorites.csv").exists()
    assert not (tmp_path / "Favorites-attachments").exists()

    assert (asset_storage / "my_locusmap_points" / expected_extracted_filename).exists()
    assert (
        asset_storage / "my_locusmap_points" / "my_locusmap_points.geojson"
    ).exists()


def test_script_e2e_points_unsupported_format(tmp_path):
    tmp_fixture_path = tmp_path / "Favorites.dxf"

    asset_storage = tmp_path / "datalake"

    with pytest.raises(ValueError):
        main(
            {"database": "test_db"},
            "my_locusmap_points",
            str(tmp_fixture_path),
            str(asset_storage),
        )
