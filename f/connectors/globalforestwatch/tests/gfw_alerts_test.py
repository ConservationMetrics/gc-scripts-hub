import psycopg2

from f.connectors.globalforestwatch.gfw_alerts import (
    main,
)


def test_script_e2e(gfw_server, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"

    main(
        gfw_server.gfw_api,
        "[[[-73.9731, 40.7644], [-73.9819, 40.7681], [-73.9580, 40.8003], [-73.9493, 40.7967], [-73.9731, 40.7644]]]",
        "gfw_integrated_alerts",
        "2025-01-01",
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
