import pytest

from f.frizzle.alerts.alerts_gcs import _main


@pytest.fixture
def mock_alerts_storage_client(gcs_service_client):
    yield gcs_service_client


def test_script(pg_database, mock_alerts_storage_client, tmp_path):
    asset_storage = tmp_path / "datalake"

    _main(
        mock_alerts_storage_client,
        "gcp-bucket",
        0,
        pg_database,
        "fake_alerts",
        asset_storage,
    )
