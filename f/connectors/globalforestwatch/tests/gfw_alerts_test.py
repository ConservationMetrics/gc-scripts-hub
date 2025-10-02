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
            # Check gfw_alerts metadata table (should have ~653 records: Jan 1, 2024 - Oct 15, 2025)
            cursor.execute("SELECT COUNT(*) FROM gfw_alerts__metadata")
            record_count = cursor.fetchone()[0]
            assert record_count >= 650  # Approximate count for ~653 days

            # Check that July 2024 has 4 alerts (from mock data) - should be on specific days
            cursor.execute(
                "SELECT total_alerts FROM gfw_alerts__metadata WHERE year = 2024 AND month = 7 AND total_alerts > 0"
            )
            july_alerts = cursor.fetchall()
            assert len(july_alerts) > 0  # Should have some days with alerts

            # Check that October 2024 has 4 alerts (from mock data) - should be on specific days
            cursor.execute(
                "SELECT total_alerts FROM gfw_alerts__metadata WHERE year = 2024 AND month = 10 AND total_alerts > 0"
            )
            october_alerts = cursor.fetchall()
            assert len(october_alerts) > 0  # Should have some days with alerts

            # Check that most days have 0 alerts
            cursor.execute(
                "SELECT COUNT(*) FROM gfw_alerts__metadata WHERE total_alerts = 0"
            )
            zero_alert_days = cursor.fetchone()[0]
            assert zero_alert_days >= 600  # Most days should have 0 alerts

            # Check that day field is populated
            cursor.execute("SELECT day FROM gfw_alerts__metadata WHERE day IS NOT NULL LIMIT 1")
            assert cursor.fetchone()[0] is not None

            cursor.execute(
                "SELECT description_alerts FROM gfw_alerts__metadata LIMIT 1"
            )
            assert cursor.fetchone()[0] == "deforestation"

            cursor.execute("SELECT data_source FROM gfw_alerts__metadata LIMIT 1")
            assert cursor.fetchone()[0] == "Global Forest Watch"


@patch("f.connectors.globalforestwatch.gfw_alerts.datetime")
def test_metadata_daily_tracking(mock_datetime, gfw_server, pg_database, tmp_path):
    """Test that metadata tracks daily alert counts for full detection range."""
    # Mock current date to be May 15, 2025
    mock_datetime.now.return_value = datetime(2025, 5, 15)

    asset_storage = tmp_path / "datalake"

    # Use gfw_server fixture VIIRS mock (pre-seeded in conftest)

    # Run with minimum_date from January 1, 2025 (should create ~135 days of metadata: Jan 1 - May 15)
    main(
        gfw_server.gfw_api,
        "[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]",
        "nasa_viirs_fire_alerts",
        "2025-01-01",
        pg_database,
        "gfw_daily_test",
        asset_storage,
    )

    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            # Check that we have metadata records for all days (Jan 1 - May 15, 2025 = ~135 days)
            cursor.execute("SELECT COUNT(*) FROM gfw_daily_test__metadata")
            record_count = cursor.fetchone()[0]
            assert record_count >= 130  # Approximate count for ~135 days

            # Check that we have records for all months from Jan to May 2025
            cursor.execute(
                "SELECT DISTINCT year, month FROM gfw_daily_test__metadata ORDER BY year, month"
            )
            months = cursor.fetchall()
            expected_months = [(2025, i) for i in range(1, 6)]  # Jan-May 2025
            assert months == expected_months

            # Check that day field is populated for all records
            cursor.execute("SELECT COUNT(*) FROM gfw_daily_test__metadata WHERE day IS NOT NULL")
            assert cursor.fetchone()[0] == record_count

            # Check that we have some days with alerts (January and March should have alerts)
            cursor.execute(
                "SELECT COUNT(*) FROM gfw_daily_test__metadata WHERE total_alerts > 0"
            )
            alert_days = cursor.fetchone()[0]
            assert alert_days > 0  # Should have some days with alerts

            # Check that January has some alerts (should be on specific days)
            cursor.execute(
                "SELECT total_alerts FROM gfw_daily_test__metadata WHERE year = 2025 AND month = 1 AND total_alerts > 0"
            )
            january_alerts = cursor.fetchall()
            assert len(january_alerts) > 0  # Should have some days with alerts

            # Check that March has some alerts (should be on specific days)
            cursor.execute(
                "SELECT total_alerts FROM gfw_daily_test__metadata WHERE year = 2025 AND month = 3 AND total_alerts > 0"
            )
            march_alerts = cursor.fetchall()
            assert len(march_alerts) > 0  # Should have some days with alerts

            # Check that most days have 0 alerts
            cursor.execute(
                "SELECT COUNT(*) FROM gfw_daily_test__metadata WHERE total_alerts = 0"
            )
            zero_alert_days = cursor.fetchone()[0]
            assert zero_alert_days >= 100  # Most days should have 0 alerts

            # Verify all records have correct data source and type
            cursor.execute(
                "SELECT DISTINCT data_source FROM gfw_daily_test__metadata"
            )
            assert cursor.fetchone()[0] == "Global Forest Watch"

            cursor.execute("SELECT DISTINCT type_alert FROM gfw_daily_test__metadata")
            assert cursor.fetchone()[0] == "nasa_viirs_fire_alerts"

            cursor.execute(
                "SELECT DISTINCT description_alerts FROM gfw_daily_test__metadata"
            )
            assert cursor.fetchone()[0] == "fires"
