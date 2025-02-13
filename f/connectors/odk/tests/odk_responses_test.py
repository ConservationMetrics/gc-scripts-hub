import psycopg2

from f.connectors.odk.odk_responses import main


def test_script_e2e(odkserver, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "odk_responses"

    main(
        odkserver.config,
        odkserver.form_id,
        pg_database,
        table_name,
        asset_storage,
    )

    # Attachments are saved to disk
    assert (asset_storage / table_name / "attachments" / "1739327186781.m4a").exists()

    # Survey responses are written to a SQL Table
    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM odk_responses")
            assert cursor.fetchone()[0] == 3

            # Check that the coordinates of a fixture entry are stored as a Point
            cursor.execute(
                "SELECT g__type, g__coordinates FROM odk_responses WHERE _id = 'uuid:cb7955d8-dc7c-480f-9699-20555a155c92'"
            )
            assert cursor.fetchone() == ("Point", "[-77.9867385, 40.322394]")
