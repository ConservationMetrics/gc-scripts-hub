import logging
import os

import google.api_core.exceptions
import pytest

from f.frizzle.alerts.alerts_gcs import _main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MOCK_BUCKET_NAME = "test-bucket"


@pytest.fixture
def mock_alerts_storage_client(gcs_emulator_client):
    """Client to a mocked Google Cloud Storage account full of alerts"""
    storage_client = gcs_emulator_client

    try:
        bucket = storage_client.create_bucket(MOCK_BUCKET_NAME)
    except google.api_core.exceptions.Conflict:
        logger.warning("Bucket already exists. Attempting to get existing bucket.")
        bucket = storage_client.bucket(MOCK_BUCKET_NAME)

    def upload_blob(bucket, source_file_name, destination_blob_name):
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        logger.warning(f"Uploaded {source_file_name} -> {destination_blob_name}")

    def upload_string_blob(bucket, string_data, destination_blob_name):
        logger.warning("BBB")
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(string_data)
        logger.info(f"Uploaded string data -> {destination_blob_name}")

    def verify_blob_exists(bucket, blob_name):
        blob = bucket.blob(blob_name)
        return blob.exists()

    # Upload test files to the emulator
    alerts_filenames = [
        "alerts_history.csv",
        "100/raster/2023/09/S1_T0_202309900112345671.tif",
        "100/raster/2023/09/S1_T1_202309900112345671.tif",
        "100/raster/2023/09/S2_T0_202309900112345671.tif",
        "100/raster/2023/09/S2_T1_202309900112345671.tif",
    ]
    assets_directory = "f/frizzle/alerts/tests/assets/"

    for filename in alerts_filenames:
        source_path = os.path.join(assets_directory, os.path.basename(filename))
        upload_blob(bucket, source_path, filename)
        assert verify_blob_exists(
            bucket, filename
        ), f"Blob {filename} was not uploaded successfully"

    # # Upload geojson alert
    # geojson_data = json.dumps(incoming_geojson_alert.incoming_geojson_alert())
    # geojson_blob_name = "100/vector/2023/09/incoming_alert.geojson"
    # try:
    #     upload_string_blob(bucket, geojson_data, geojson_blob_name)
    #     assert verify_blob_exists(
    #         bucket, geojson_blob_name
    #     ), f"Blob {geojson_blob_name} was not uploaded successfully"
    # except Exception as e:
    #     logger.error(f"Failed to upload or verify {geojson_blob_name}: {e}")
    #     raise

    yield storage_client


def test_script(pg_database, mock_alerts_storage_client, tmp_path):
    asset_storage = tmp_path / "datalake"

    _main(
        mock_alerts_storage_client,
        MOCK_BUCKET_NAME,
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
    )
