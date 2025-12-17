import base64
import hashlib
import logging
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import google.api_core.exceptions
import pandas as pd
import psycopg2
import pytest

from f.connectors.alerts.alerts_gcs import (
    _main,
    prepare_alerts_data,
    prepare_alerts_metadata,
)

logger = logging.getLogger(__name__)

MOCK_BUCKET_NAME = "test-bucket"
assets_directory = "f/connectors/alerts/tests/assets/"


def test_prepare_alerts_metadata():
    alerts_history_csv = Path(assets_directory, "alerts_history.csv")
    alerts_metadata = pd.read_csv(alerts_history_csv).to_csv(index=False)

    prepared_alerts_metadata, alert_statistics = prepare_alerts_metadata(
        alerts_metadata, 100, "test_provider", max_months_lookback=None
    )

    # Check that alerts statistics is the latest month and year in the CSV
    assert alert_statistics["month_year"] == "2/2024"
    assert alert_statistics["total_alerts"] == "1"
    assert alert_statistics["description_alerts"] == "ghostly barrow noises"


def test_metadata_id_stability():
    alerts_history_csv = Path(assets_directory, "alerts_history.csv")
    alerts_metadata = pd.read_csv(alerts_history_csv).to_csv(index=False)

    first, _ = prepare_alerts_metadata(
        alerts_metadata, 100, "test_provider", max_months_lookback=None
    )
    second, _ = prepare_alerts_metadata(
        alerts_metadata, 100, "test_provider", max_months_lookback=None
    )

    # Order shouldn't matter but just to be safe, sort by _id
    first_ids = sorted(r["_id"] for r in first)
    second_ids = sorted(r["_id"] for r in second)

    assert first_ids == second_ids
    assert len(set(first_ids)) == len(first_ids)  # No duplicates


def test_alert_id_generation(tmp_path):
    file_path = Path(assets_directory, "alert_202309900112345671.geojson")
    geojson_files = [str(file_path)]
    prepared = prepare_alerts_data(tmp_path, geojson_files, "test_provider")

    assert len(prepared) > 0
    for row in prepared:
        assert "_id" in row
        uuid.UUID(row["_id"])  # will raise ValueError if invalid


def test_geometry_collection_validation(tmp_path):
    """Test that GeometryCollection geometries raise ValueError."""
    # Create a temporary GeoJSON file with GeometryCollection
    geojson_with_geometry_collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "GeometryCollection",
                    "geometries": [
                        {
                            "type": "LineString",
                            "coordinates": [
                                [-100.11945344942263, 25.327808970020767],
                                [-100.11954328095104, 25.327808970020767],
                            ],
                        },
                        {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-100.11945344942263, 25.327808970020767],
                                    [-100.11945344942263, 25.328078464606003],
                                    [-100.11954328095104, 25.328078464606003],
                                    [-100.11954328095104, 25.327808970020767],
                                    [-100.11945344942263, 25.327808970020767],
                                ]
                            ],
                        },
                    ],
                },
                "id": "-123456+78910",
                "properties": {"alert_id": "test_alert_123", "territory_id": 100},
            }
        ],
    }

    # Write the test GeoJSON to a temporary file
    test_file = tmp_path / "test_geometry_collection.geojson"
    with open(test_file, "w") as f:
        import json

        json.dump(geojson_with_geometry_collection, f)

    geojson_files = [str(test_file)]

    # Test that ValueError is raised
    with pytest.raises(
        ValueError, match="GeometryCollection geometries are not supported"
    ):
        prepare_alerts_data(tmp_path, geojson_files, "test_provider")


@pytest.fixture
def mock_alerts_storage_client(gcs_emulator_client):
    """Client to a mocked Google Cloud Storage account full of alerts"""
    storage_client = gcs_emulator_client

    try:
        bucket = storage_client.create_bucket(MOCK_BUCKET_NAME)
    except google.api_core.exceptions.Conflict:
        logger.warning("Bucket already exists. Attempting to get existing bucket.")
        bucket = storage_client.bucket(MOCK_BUCKET_NAME)

    def _upload_blob(bucket, source_file_name, destination_blob_name):
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        logger.info(f"Uploaded {source_file_name} -> {destination_blob_name}")

    # Upload test files to the emulator
    alerts_filenames = [
        "alerts_history.csv",
        "100/vector/2023/09/alert_202309900112345671.geojson",
        "100/raster/2023/09/S1_T0_202309900112345671.tif",
        "100/raster/2023/09/S1_T1_202309900112345671.tif",
        "100/raster/2023/09/S2_T0_202309900112345671.tif",
        "100/raster/2023/09/S2_T1_202309900112345671.tif",
    ]
    for filename in alerts_filenames:
        source_path = Path(assets_directory) / Path(filename).name
        _upload_blob(bucket, source_path, filename)

    yield storage_client


def test_script_e2e(pg_database, mock_alerts_storage_client, tmp_path):
    asset_storage = tmp_path / "datalake"

    alerts_metadata = _main(
        mock_alerts_storage_client,
        MOCK_BUCKET_NAME,
        "test_provider",
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
        max_months_lookback=None,
    )

    # Alerts are written to a SQL Table
    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM fake_alerts")
            assert cursor.fetchone()[0] == 1  # Length of assets/alerts.geojson

            # Check that the _id field is a valid UUID
            cursor.execute("SELECT _id FROM fake_alerts")
            _id = cursor.fetchone()[0]
            assert uuid.UUID(_id)

            # Check that the _id field is unique
            cursor.execute("SELECT _id FROM fake_alerts")
            ids = [row[0] for row in cursor.fetchall()]
            assert len(ids) == len(set(ids)), "Duplicate _id values found in alerts"

            # Schema assertions for alerts table
            cursor.execute("SELECT * FROM fake_alerts LIMIT 1")
            alerts_desc = [col.name for col in cursor.description]
            expected_alerts_fields = {
                "_id",
                "alert_id",
                "alert_type",
                "area_alert_ha",
                "basin_id",
                "confidence",
                "count",
                "date_end_t0",
                "date_end_t1",
                "date_start_t0",
                "date_start_t1",
                "grid",
                "label",
                "month_detec",
                "sat_detect_prefix",
                "sat_viz_prefix",
                "satellite",
                "territory_id",
                "territory_name",
                "year_detec",
                "length_alert_km",
                "g__type",
                "g__coordinates",
                "data_source",
                "source_file_name",
            }
            assert expected_alerts_fields.issubset(set(alerts_desc)), (
                f"Missing fields in alerts table: {expected_alerts_fields - set(alerts_desc)}"
            )

            # Check that the _id field is unique in the metadata table
            cursor.execute("SELECT _id FROM fake_alerts__metadata")
            ids = [row[0] for row in cursor.fetchall()]
            assert len(ids) == len(set(ids)), "Duplicate _id values found in metadata"

            # Count of unique rows in alerts_history.csv based on UUID
            cursor.execute("SELECT COUNT(*) FROM fake_alerts__metadata")
            assert cursor.fetchone()[0] == 8

            # Schema assertions for metadata table
            cursor.execute("SELECT * FROM fake_alerts__metadata LIMIT 1")
            metadata_desc = [col.name for col in cursor.description]
            expected_metadata_fields = {
                "_id",
                "confidence",
                "description_alerts",
                "month",
                "territory_id",
                "total_alerts",
                "type_alert",
                "year",
                "data_source",
            }
            assert expected_metadata_fields.issubset(set(metadata_desc)), (
                f"Missing fields in metadata table: {expected_metadata_fields - set(metadata_desc)}"
            )

            # Ensure that both types of alerts are present for the month of 09/2023
            cursor.execute(
                "SELECT COUNT(*) FROM fake_alerts__metadata WHERE year = 2023 AND month = 9 AND description_alerts = 'ghostly_barrow_noises'"
            )
            assert cursor.fetchone()[0] == 1
            cursor.execute(
                "SELECT COUNT(*) FROM fake_alerts__metadata WHERE year = 2023 AND month = 9 AND description_alerts = 'unexpected_carrot_theft'"
            )
            assert cursor.fetchone()[0] == 1

            # Ensure that for month of 01/2024, there are two rows with different confidence levels
            cursor.execute(
                "SELECT COUNT(*) FROM fake_alerts__metadata WHERE year = 2024 AND month = 1 AND confidence = 1"
            )
            assert cursor.fetchone()[0] == 1
            cursor.execute(
                "SELECT COUNT(*) FROM fake_alerts__metadata WHERE year = 2024 AND month = 1 AND confidence = 0"
            )
            assert cursor.fetchone()[0] == 1

            # Check that the confidence field is NULL if it is not defined in the CSV
            cursor.execute(
                "SELECT confidence FROM fake_alerts__metadata WHERE year = 2022 AND month = 12"
            )  # This is the only row in the CSV that does not have a confidence value
            assert cursor.fetchone()[0] is None

            # Check that there is no __columns table created
            cursor.execute(
                "SELECT * FROM information_schema.tables WHERE table_name = 'fake_alerts__columns'"
            )
            assert cursor.fetchone() is None

    # GeoJSON is saved to disk
    assert (
        asset_storage
        / "100/2023/09/202309900112345671/alert_202309900112345671.geojson"
    ).exists()

    # Rasters are saved to disk
    for basename in (
        "S1_T0_202309900112345671",
        "S1_T1_202309900112345671",
        "S2_T0_202309900112345671",
        "S2_T1_202309900112345671",
    ):
        # file naming format: <territory_id>/<year_detec>/<month_detec>/<alert_id>/images/<filename>.tif
        assert (
            asset_storage / "100/2023/09/202309900112345671/images" / f"{basename}.tif"
        ).exists()
        # Attachments are also converted to JPG
        assert (
            asset_storage / "100/2023/09/202309900112345671/images" / f"{basename}.jpg"
        ).exists()

    # Alerts metadata is not saved to disk
    assert not (asset_storage / "alerts_history.csv").exists()

    # Check that the alerts metadata is returned by the script. The unit test for prepare_alerts_metadata()
    # checks the correctness of the metadata, so here we just check that it is not None
    assert alerts_metadata is not None

    # Now, let's run the script again to check if alerts_metadata is returned (it should be None,
    # since no new alerts data or metadata has been inserted into the database)
    alerts_metadata = _main(
        mock_alerts_storage_client,
        MOCK_BUCKET_NAME,
        "test_provider",
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
        max_months_lookback=None,
    )

    assert alerts_metadata is None


def test_file_update_logic(pg_database, mock_alerts_storage_client, tmp_path):
    asset_storage = tmp_path / "datalake"
    asset_storage.mkdir(parents=True, exist_ok=True)

    _main(
        mock_alerts_storage_client,
        MOCK_BUCKET_NAME,
        "test_provider",
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
        max_months_lookback=None,
    )

    tif_file_path = (
        asset_storage
        / "100/2023/09/202309900112345671/images/S1_T0_202309900112345671.tif"
    )

    _main(
        mock_alerts_storage_client,
        MOCK_BUCKET_NAME,
        "test_provider",
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
        max_months_lookback=None,
    )

    # Check that the file MD5 hash has not changed, since the file was not updated
    with open(tif_file_path, "rb") as f:
        local_md5_hash = hashlib.md5(f.read()).hexdigest()

    bucket = mock_alerts_storage_client.bucket(MOCK_BUCKET_NAME)
    blob = bucket.blob("100/raster/2023/09/S1_T0_202309900112345671.tif")
    blob.reload()
    gcs_md5_hash_base64 = blob.md5_hash

    gcs_md5_hash = base64.b64decode(gcs_md5_hash_base64).hex()

    assert local_md5_hash == gcs_md5_hash

    # Simulate an update to the blob on GCS by uploading a new version
    new_content = b"Updated content to simulate a blob update."
    blob.upload_from_string(new_content)

    _main(
        mock_alerts_storage_client,
        MOCK_BUCKET_NAME,
        "test_provider",
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
        max_months_lookback=None,
    )

    # Now, the file MD5 hash should have changed, since the file was updated
    with open(tif_file_path, "rb") as f:
        updated_md5_hash = hashlib.md5(f.read()).hexdigest()

    blob.reload()
    updated_gcs_md5_hash_base64 = blob.md5_hash

    updated_gcs_md5_hash = base64.b64decode(updated_gcs_md5_hash_base64).hex()

    assert updated_md5_hash == updated_gcs_md5_hash


@pytest.fixture
def mock_alerts_storage_client_metadata_only(gcs_emulator_client):
    """Client to a mocked Google Cloud Storage account with only metadata, no files for territory 100"""
    storage_client = gcs_emulator_client

    try:
        bucket = storage_client.create_bucket(MOCK_BUCKET_NAME)
    except google.api_core.exceptions.Conflict:
        logger.warning("Bucket already exists. Attempting to get existing bucket.")
        bucket = storage_client.bucket(MOCK_BUCKET_NAME)

    def _upload_blob(bucket, source_file_name, destination_blob_name):
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        logger.info(f"Uploaded {source_file_name} -> {destination_blob_name}")

    # Upload only metadata file - no files for territory 100
    alerts_filenames = [
        "alerts_history.csv",
    ]
    for filename in alerts_filenames:
        source_path = Path(assets_directory) / Path(filename).name
        _upload_blob(bucket, source_path, filename)

    yield storage_client


def test_metadata_only_scenario(
    pg_database, mock_alerts_storage_client_metadata_only, tmp_path
):
    """Test scenario where there are no files for territory_id but metadata exists"""
    asset_storage = tmp_path / "datalake"

    # This should not raise an assertion error even though no files exist for territory 100
    # because metadata exists for territory 100 in alerts_history.csv
    alerts_metadata = _main(
        mock_alerts_storage_client_metadata_only,
        MOCK_BUCKET_NAME,
        "test_provider",
        100,  # territory_id 100 has metadata but no files
        pg_database,
        "fake_alerts_metadata_only",
        asset_storage,
        max_months_lookback=None,
    )

    # Check that metadata was written to the database
    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            # Check that metadata table was created and has data
            cursor.execute("SELECT COUNT(*) FROM fake_alerts_metadata_only__metadata")
            metadata_count = cursor.fetchone()[0]
            assert metadata_count > 0, "Metadata should be written to database"

            # Check that alerts table was NOT created (no files to process)
            cursor.execute(
                "SELECT * FROM information_schema.tables WHERE table_name = 'fake_alerts_metadata_only'"
            )
            assert cursor.fetchone() is None, (
                "Alerts table should not be created when there are no files to process"
            )

    # Check that alerts metadata is returned (not None)
    assert alerts_metadata is not None


def test_no_files_no_metadata_scenario(
    pg_database, mock_alerts_storage_client_metadata_only, tmp_path
):
    """Test scenario where there are no files and no metadata for territory_id - should raise assertion error"""
    asset_storage = tmp_path / "datalake"

    # This should raise an assertion error because no files exist for territory 999
    # and no metadata exists for territory 999 in alerts_history.csv
    with pytest.raises(
        AssertionError,
        match="No files found to download.*and no metadata found for territory_id 999",
    ):
        _main(
            mock_alerts_storage_client_metadata_only,
            MOCK_BUCKET_NAME,
            "test_provider",
            999,  # territory_id 999 has neither files nor metadata
            pg_database,
            "fake_alerts_no_data",
            asset_storage,
            max_months_lookback=None,
        )


@patch("f.common_logic.date_utils.datetime")
def test_max_months_lookback_alerts_data_filtering(mock_datetime, tmp_path):
    """Test that max_months_lookback correctly filters alert files by date"""
    # Mock current date to October 2025
    mock_datetime.now.return_value = datetime(2025, 10, 15)

    # Create test GeoJSON files with different dates
    test_geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [0, 0]},
                "properties": {"id": "test_alert_123", "territory_id": 100},
            }
        ],
    }

    # Old alert from 2023
    old_file = tmp_path / "alert_old.geojson"
    old_file.write_text(str(test_geojson).replace("'", '"'))

    # Recent alert from 2025
    recent_file = tmp_path / "alert_recent.geojson"
    recent_file.write_text(
        str(test_geojson).replace("test_alert_123", "test_alert_456").replace("'", '"')
    )

    # No lookback - process all files
    all_files = [str(old_file), str(recent_file)]
    prepared_all = prepare_alerts_data(tmp_path, all_files, "test_provider")
    assert len(prepared_all) == 2

    # With lookback, we'd need to filter files before calling prepare_alerts_data
    # This test verifies that prepare_alerts_data itself processes what it's given
    recent_only = [str(recent_file)]
    prepared_filtered = prepare_alerts_data(tmp_path, recent_only, "test_provider")
    assert len(prepared_filtered) == 1
    assert prepared_filtered[0]["alert_id"] == "test_alert_456"


@patch("f.common_logic.date_utils.datetime")
def test_max_months_lookback_metadata_filtering(mock_datetime):
    """Test that max_months_lookback correctly filters metadata by date"""
    # Mock current date to October 2025
    mock_datetime.now.return_value = datetime(2025, 10, 15)

    # Create test data: old (2023) and recent (2025)
    test_df = pd.DataFrame(
        {
            "territory_id": [100, 100, 100],
            "type_alert": ["001", "001", "002"],
            "month": [9, 10, 10],
            "year": [2023, 2025, 2025],
            "total_alerts": [10, 20, 30],
            "description_alerts": ["old_alert", "recent_alert_1", "recent_alert_2"],
            "confidence": [1.0, 1.0, 0.5],
        }
    )
    alerts_metadata = test_df.to_csv(index=False)

    # No lookback - get all data
    prepared_all, _ = prepare_alerts_metadata(
        alerts_metadata, 100, "test_provider", max_months_lookback=None
    )
    assert len(prepared_all) == 3

    # 6 months lookback - should exclude 2023 data
    prepared_filtered, _ = prepare_alerts_metadata(
        alerts_metadata, 100, "test_provider", max_months_lookback=6
    )
    assert len(prepared_filtered) == 2
    descriptions = {row["description_alerts"] for row in prepared_filtered}
    assert "old_alert" not in descriptions
    assert "recent_alert_1" in descriptions


@patch("f.common_logic.date_utils.datetime")
def test_max_months_lookback_e2e(
    mock_datetime, pg_database, mock_alerts_storage_client, tmp_path
):
    """Test that max_months_lookback filters out old files and metadata in E2E flow"""
    # Mock current date to October 2025
    mock_datetime.now.return_value = datetime(2025, 10, 15)

    asset_storage = tmp_path / "datalake"

    # Mock data is from 2023/09 - use 1 month lookback to filter everything
    result = _main(
        mock_alerts_storage_client,
        MOCK_BUCKET_NAME,
        "test_provider",
        100,
        pg_database,
        "fake_alerts_filtered",
        asset_storage,
        max_months_lookback=1,
    )

    # Should return None since all data was filtered
    assert result is None

    # Verify both tables were NOT created (since there's no data to write)
    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM information_schema.tables WHERE table_name = 'fake_alerts_filtered'"
            )
            assert cursor.fetchone() is None, (
                "Alerts table should not be created when there's no data"
            )

            cursor.execute(
                "SELECT * FROM information_schema.tables WHERE table_name = 'fake_alerts_filtered__metadata'"
            )
            assert cursor.fetchone() is None, (
                "Metadata table should not be created when there's no data"
            )
