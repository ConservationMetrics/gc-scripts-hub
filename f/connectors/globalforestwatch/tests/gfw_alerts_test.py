from datetime import datetime
from unittest.mock import patch

import psycopg2

from f.connectors.globalforestwatch.gfw_alerts import (
    main,
)


@patch("f.connectors.globalforestwatch.gfw_alerts.datetime")
def test_script_e2e(mock_datetime, gfw_server, pg_database, tmp_path):
    # Mock current date to be October 2025
    mock_datetime.now.return_value = datetime(2025, 10, 15)

    asset_storage = tmp_path / "datalake"

    main(
        gfw_server.gfw_api,
        "[[[-73.9731, 40.7644], [-73.9819, 40.7681], [-73.9580, 40.8003], [-73.9493, 40.7967], [-73.9731, 40.7644]]]",
        "gfw_integrated_alerts",
        "2024-01-01",
        pg_database,
        "gfw_alerts",
        asset_storage,
    )

    # GeoJSON file is saved to disk
    assert (asset_storage / "gfw_alerts" / "gfw_alerts.geojson").exists()

    with psycopg2.connect(**pg_database) as conn:
        # Survey responses from gfw_alerts are written to a SQL Table in expected format
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM gfw_alerts")
            assert cursor.fetchone()[0] == 8

            cursor.execute("SELECT g__type FROM gfw_alerts LIMIT 1")
            assert cursor.fetchone()[0] == "Point"

            cursor.execute("SELECT g__coordinates FROM gfw_alerts LIMIT 1")
            assert cursor.fetchone()[0] == "[-55.22595, 5.70545]"

            cursor.execute("SELECT confidence FROM gfw_alerts LIMIT 1 OFFSET 1")
            assert cursor.fetchone()[0] == "medium"

    # Test metadata tables are created and populated
    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            # Check gfw_alerts metadata table (should have 22 records: Jan 2024 - Oct 2025)
            cursor.execute("SELECT COUNT(*) FROM gfw_alerts__metadata")
            assert cursor.fetchone()[0] == 22

            # Check that July 2024 has 4 alerts (from mock data)
            cursor.execute(
                "SELECT total_alerts FROM gfw_alerts__metadata WHERE year = 2024 AND month = 7"
            )
            assert cursor.fetchone()[0] == 4

            # Check that October 2024 has 4 alerts (from mock data)
            cursor.execute(
                "SELECT total_alerts FROM gfw_alerts__metadata WHERE year = 2024 AND month = 10"
            )
            assert cursor.fetchone()[0] == 4

            # Check that other months have 0 alerts
            cursor.execute(
                "SELECT COUNT(*) FROM gfw_alerts__metadata WHERE total_alerts = 0"
            )
            assert cursor.fetchone()[0] == 20  # 20 months with 0 alerts

            cursor.execute(
                "SELECT description_alerts FROM gfw_alerts__metadata LIMIT 1"
            )
            assert cursor.fetchone()[0] == "deforestation"

            cursor.execute("SELECT data_source FROM gfw_alerts__metadata LIMIT 1")
            assert cursor.fetchone()[0] == "Global Forest Watch"


@patch("f.connectors.globalforestwatch.gfw_alerts.datetime")
def test_metadata_monthly_tracking(mock_datetime, gfw_server, pg_database, tmp_path):
    """Test that metadata tracks monthly alert counts for full detection range."""
    # Mock current date to be May 2025
    mock_datetime.now.return_value = datetime(2025, 5, 15)

    asset_storage = tmp_path / "datalake"

    # Use gfw_server fixture VIIRS mock (pre-seeded in conftest)

    # Run with minimum_date from January 2025 (should create 5 months of metadata: Jan-May)
    main(
        gfw_server.gfw_api,
        "[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]",
        "nasa_viirs_fire_alerts",
        "2025-01-01",
        pg_database,
        "gfw_monthly_test",
        asset_storage,
    )

    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            # Check that we have metadata records for all months (Jan-May 2025 = 5 months)
            cursor.execute("SELECT COUNT(*) FROM gfw_monthly_test__metadata")
            assert cursor.fetchone()[0] == 5

            # Check that we have records for all months from Jan to May 2025
            cursor.execute(
                "SELECT DISTINCT year, month FROM gfw_monthly_test__metadata ORDER BY year, month"
            )
            months = cursor.fetchall()
            expected_months = [(2025, i) for i in range(1, 6)]  # Jan-May 2025
            assert months == expected_months

            # Check January has 2 alerts
            cursor.execute(
                "SELECT total_alerts FROM gfw_monthly_test__metadata WHERE year = 2025 AND month = 1"
            )
            assert cursor.fetchone()[0] == 2

            # Check February has 0 alerts
            cursor.execute(
                "SELECT total_alerts FROM gfw_monthly_test__metadata WHERE year = 2025 AND month = 2"
            )
            assert cursor.fetchone()[0] == 0

            # Check March has 3 alerts
            cursor.execute(
                "SELECT total_alerts FROM gfw_monthly_test__metadata WHERE year = 2025 AND month = 3"
            )
            assert cursor.fetchone()[0] == 3

            # Check April has 0 alerts
            cursor.execute(
                "SELECT total_alerts FROM gfw_monthly_test__metadata WHERE year = 2025 AND month = 4"
            )
            assert cursor.fetchone()[0] == 0

            # Check May has 0 alerts
            cursor.execute(
                "SELECT total_alerts FROM gfw_monthly_test__metadata WHERE year = 2025 AND month = 5"
            )
            assert cursor.fetchone()[0] == 0

            # Verify all records have correct data source and type
            cursor.execute(
                "SELECT DISTINCT data_source FROM gfw_monthly_test__metadata"
            )
            assert cursor.fetchone()[0] == "Global Forest Watch"

            cursor.execute("SELECT DISTINCT type_alert FROM gfw_monthly_test__metadata")
            assert cursor.fetchone()[0] == "nasa_viirs_fire_alerts"

            cursor.execute(
                "SELECT DISTINCT description_alerts FROM gfw_monthly_test__metadata"
            )
            assert cursor.fetchone()[0] == "fires"
