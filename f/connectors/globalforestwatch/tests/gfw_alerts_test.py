from datetime import datetime
from unittest.mock import patch

import psycopg2

from f.connectors.globalforestwatch.gfw_alerts import (
    main,
    prepare_gfw_metadata,
)


@patch("f.connectors.globalforestwatch.gfw_alerts.datetime")
@patch("f.common_logic.date_utils.datetime")
def test_script_e2e(mock_datetime_utils, mock_datetime_gfw, gfw_server, pg_database, tmp_path):
    # Mock current date to be October 2025
    mock_datetime_utils.now.return_value = datetime(2025, 10, 15)
    mock_datetime_gfw.now.return_value = datetime(2025, 10, 15)

    asset_storage = tmp_path / "datalake"

    # Fetch alerts from last 22 months (roughly equivalent to 2024-01-01 start)
    main(
        gfw_server.gfw_api,
        "[[[-73.9731, 40.7644], [-73.9819, 40.7681], [-73.9580, 40.8003], [-73.9493, 40.7967], [-73.9731, 40.7644]]]",
        "gfw_integrated_alerts",
        22,  # ~22 months covers from Dec 2023 to Oct 2025
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
            # Check gfw_alerts metadata table (should have ~685 records: Dec 1, 2023 - Oct 15, 2025)
            cursor.execute("SELECT COUNT(*) FROM gfw_alerts__metadata")
            record_count = cursor.fetchone()[0]
            assert record_count >= 680  # Approximate count for ~685 days

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
@patch("f.common_logic.date_utils.datetime")
def test_metadata_daily_tracking(
    mock_datetime_utils, mock_datetime_gfw, gfw_server, pg_database, tmp_path
):
    """Test that metadata tracks daily alert counts for full detection range."""
    # Mock current date to be May 15, 2025
    mock_datetime_utils.now.return_value = datetime(2025, 5, 15)
    mock_datetime_gfw.now.return_value = datetime(2025, 5, 15)

    asset_storage = tmp_path / "datalake"

    # Use gfw_server fixture VIIRS mock (pre-seeded in conftest)

    # Run with 5 months lookback (should create ~135 days of metadata: Jan 1 - May 15)
    main(
        gfw_server.gfw_api,
        "[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]",
        "nasa_viirs_fire_alerts",
        5,  # 5 months back from May 15 = Jan 1
        pg_database,
        "gfw_daily_test",
        asset_storage,
    )

    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            # Check that we have metadata records for all days (Dec 1, 2024 - May 15, 2025 = ~165 days)
            # 5 months back from May 15 = Dec 15, start from Dec 1
            cursor.execute("SELECT COUNT(*) FROM gfw_daily_test__metadata")
            record_count = cursor.fetchone()[0]
            assert record_count >= 160  # Approximate count for ~165 days

            # Check that we have records for all months from Dec 2024 to May 2025
            cursor.execute(
                "SELECT DISTINCT year, month FROM gfw_daily_test__metadata ORDER BY year, month"
            )
            months = cursor.fetchall()
            expected_months = [(2024, 12)] + [(2025, i) for i in range(1, 6)]  # Dec 2024 - May 2025
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


@patch("f.common_logic.date_utils.datetime")
def test_max_months_lookback_metadata_filtering(mock_datetime):
    """Test that max_months_lookback correctly filters metadata by date range"""
    # Mock current date to October 2025
    mock_datetime.now.return_value = datetime(2025, 10, 15)

    # Create mock alerts from different dates
    alerts = [
        {"alert__date": "2024-01-15"},  # Old - 21 months ago
        {"alert__date": "2025-07-10"},  # Recent - 3 months ago
        {"alert__date": "2025-10-01"},  # Very recent - current month
    ]

    # 22 months lookback - processes from ~Dec 2023 to now
    prepared_all = prepare_gfw_metadata(alerts, "nasa_viirs_fire_alerts", 22)
    # Should have ~685 days (Dec 1, 2023 to Oct 15, 2025)
    assert len(prepared_all) >= 680

    # 6 months lookback - only process last 6+ months
    prepared_filtered = prepare_gfw_metadata(alerts, "nasa_viirs_fire_alerts", 6)
    # Should have ~198-210 days (April 1 to Oct 15, 2025)
    assert len(prepared_filtered) < 220
    assert len(prepared_filtered) >= 190

    # Verify only recent months are included
    years_months = {(record["year"], record["month"]) for record in prepared_filtered}
    # Should not include January 2024 (too old)
    assert (2024, 1) not in years_months
    # Should include recent months like July, August, September, October 2025
    assert (2025, 10) in years_months


@patch("f.connectors.globalforestwatch.gfw_alerts.datetime")
@patch("f.common_logic.date_utils.datetime")
def test_max_months_lookback_e2e(
    mock_datetime_utils, mock_datetime_gfw, gfw_server, pg_database, tmp_path
):
    """Test that max_months_lookback limits API query and metadata records in E2E flow"""
    # Mock current date to October 2025
    mock_datetime_utils.now.return_value = datetime(2025, 10, 15)
    mock_datetime_gfw.now.return_value = datetime(2025, 10, 15)

    asset_storage = tmp_path / "datalake"

    # Run with 3 months lookback - should query API from July 1, 2025
    # and create ~107 days of metadata (July 1 - Oct 15, 2025)
    main(
        gfw_server.gfw_api,
        "[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]",
        "nasa_viirs_fire_alerts",
        3,  # 3 months lookback
        pg_database,
        "gfw_lookback_test",
        asset_storage,
    )

    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            # Should have ~107 days (July 1 - Oct 15, 2025)
            # Note: cutoff starts from first day of month
            cursor.execute("SELECT COUNT(*) FROM gfw_lookback_test__metadata")
            record_count = cursor.fetchone()[0]
            assert record_count < 115
            assert record_count >= 100

            # Verify only recent months are included
            cursor.execute(
                "SELECT DISTINCT year, month FROM gfw_lookback_test__metadata ORDER BY year, month"
            )
            months = cursor.fetchall()
            # Should only have July-October 2025
            for year, month in months:
                assert year == 2025
                assert month >= 7  # July or later
