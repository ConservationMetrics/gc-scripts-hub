import logging
import os

import google.api_core.exceptions
import psycopg2
import pytest

from f.connectors.alerts.alerts_gcs import _main

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

    def _upload_blob(bucket, source_file_name, destination_blob_name):
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        logger.info(f"Uploaded {source_file_name} -> {destination_blob_name}")

    # Upload test files to the emulator
    assets_directory = "f/connectors/alerts/tests/assets/"
    alerts_filenames = [
        "alerts_history.csv",
        "100/vector/2023/09/alert_202309900112345671.geojson",
        "100/raster/2023/09/S1_T0_202309900112345671.tif",
        "100/raster/2023/09/S1_T1_202309900112345671.tif",
        "100/raster/2023/09/S2_T0_202309900112345671.tif",
        "100/raster/2023/09/S2_T1_202309900112345671.tif",
    ]
    for filename in alerts_filenames:
        source_path = os.path.join(assets_directory, os.path.basename(filename))
        _upload_blob(bucket, source_path, filename)

    yield storage_client


def test_script_e2e(pg_database, mock_alerts_storage_client, tmp_path):
    asset_storage = tmp_path / "datalake"

    _main(
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

            # Count of unique rows in alerts_history.csv based on UUID
            # The last row in the CSV is a duplicate of the one before it, but updates the confidence field, hence shares the same UUID
            cursor.execute("SELECT COUNT(*) FROM fake_alerts__metadata")
            assert cursor.fetchone()[0] == 5

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
    # Log initial timestamp and download time of the file
    initial_timestamp = tif_file_path.stat().st_mtime
    initial_download_time = os.path.getctime(tif_file_path)

    _main(
        mock_alerts_storage_client,
        MOCK_BUCKET_NAME,
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
    )

    # Check that the file timestamp and download time have not changed,
    # since the file was not updated
    assert tif_file_path.stat().st_mtime == initial_timestamp
    assert os.path.getctime(tif_file_path) == initial_download_time

    # Modify the file to simulate an update
    with open(tif_file_path, "a") as f:
        f.write("Modified content to simulate a file update.")

    _main(
        mock_alerts_storage_client,
        MOCK_BUCKET_NAME,
        100,
        pg_database,
        "fake_alerts",
        asset_storage,
    )

    # Now, the file timestamp and download time should
    # have changed, since the file was updated
    updated_timestamp = tif_file_path.stat().st_mtime
    assert updated_timestamp > initial_timestamp
    assert os.path.getctime(tif_file_path) > initial_download_time
