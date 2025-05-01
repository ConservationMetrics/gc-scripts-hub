import base64
import hashlib
import logging
import uuid
from pathlib import Path

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
        alerts_metadata, 100
    )

    # Check that alerts statistics is the latest month and year in the CSV
    assert alert_statistics["month_year"] == "2/2024"
    assert alert_statistics["total_alerts"] == "1"
    assert alert_statistics["description_alerts"] == "ghostly barrow noises"


def test_metadata_id_stability():
    alerts_history_csv = Path(assets_directory, "alerts_history.csv")
    alerts_metadata = pd.read_csv(alerts_history_csv).to_csv(index=False)

    first, _ = prepare_alerts_metadata(alerts_metadata, 100)
    second, _ = prepare_alerts_metadata(alerts_metadata, 100)

    # Order shouldn't matter but just to be safe, sort by _id
    first_ids = sorted(r["_id"] for r in first)
    second_ids = sorted(r["_id"] for r in second)

    assert first_ids == second_ids
    assert len(set(first_ids)) == len(first_ids)  # No duplicates


def test_alert_id_generation(tmp_path):
    file_path = Path(assets_directory, "alert_202309900112345671.geojson")
    geojson_files = [str(file_path)]
    prepared = prepare_alerts_data(tmp_path, geojson_files)

    assert len(prepared) > 0
    for row in prepared:
        assert "_id" in row
        uuid.UUID(row["_id"])  # will raise ValueError if invalid


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
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
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
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
    )

    assert alerts_metadata is None


def test_file_update_logic(pg_database, mock_alerts_storage_client, tmp_path):
    asset_storage = tmp_path / "datalake"
    asset_storage.mkdir(parents=True, exist_ok=True)

    _main(
        mock_alerts_storage_client,
        MOCK_BUCKET_NAME,
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
    )

    tif_file_path = (
        asset_storage
        / "100/2023/09/202309900112345671/images/S1_T0_202309900112345671.tif"
    )

    _main(
        mock_alerts_storage_client,
        MOCK_BUCKET_NAME,
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
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
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
    )

    # Now, the file MD5 hash should have changed, since the file was updated
    with open(tif_file_path, "rb") as f:
        updated_md5_hash = hashlib.md5(f.read()).hexdigest()

    blob.reload()
    updated_gcs_md5_hash_base64 = blob.md5_hash

    updated_gcs_md5_hash = base64.b64decode(updated_gcs_md5_hash_base64).hex()

    assert updated_md5_hash == updated_gcs_md5_hash
