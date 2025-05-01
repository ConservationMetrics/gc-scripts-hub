import psycopg2

from f.connectors.kobotoolbox.kobotoolbox_responses import main


def test_script_e2e(koboserver, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "kobo_responses"

    main(
        koboserver.account,
        koboserver.form_id,
        pg_database,
        table_name,
        asset_storage,
    )

    # Attachments are saved to disk
    assert (asset_storage / table_name / "attachments" / "1637241249813.jpg").exists()

    # Survey responses are written to a SQL Table
    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM kobo_responses")
            assert cursor.fetchone()[0] == 3

            # Check that the coordinates of a fixture entry are stored as a Point,
            # and that the coordinates are reversed (longitude, latitude).
            cursor.execute(
                "SELECT g__type, g__coordinates FROM kobo_responses WHERE _id = '124961136'"
            )
            assert cursor.fetchone() == ("Point", "[-122.0109429, 36.97012]")

            # Check that meta/instanceID was sanitized to instanceID__meta
            cursor.execute(
                "SELECT \"instanceID__meta\" FROM kobo_responses WHERE _id = '124961136'"
            )
            assert cursor.fetchone() == ("uuid:e58da38d-3eee-4bd7-8512-4a97ea8fbb01",)

            # Check that the mapping column was created
            cursor.execute(
                "SELECT COUNT(*) FROM kobo_responses__columns WHERE original_column = 'meta/instanceID' AND sql_column = 'instanceID__meta'"
            )
            assert cursor.fetchone()[0] == 1
