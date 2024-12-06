import psycopg2

from f.frizzle.comapeo.comapeo_observations import main


def test_script(comapeoserver, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"

    main(
        comapeoserver.comapeo_server,
        comapeoserver.comapeo_project_blocklist,
        pg_database,
        None,
        asset_storage,
    )

    # Attachments are saved to disk
    assert (
        asset_storage / "comapeo" / "forest_expedition" / "attachments" / "capybara.jpg"
    ).exists()

    with psycopg2.connect(**pg_database) as conn:
        # Survey responses from forest_expedition are written to a SQL Table
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM comapeo_forest_expedition")
            assert cursor.fetchone()[0] == 3

        # comapeo_river_mapping SQL Table does not exist (it's in the blocklist)
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'comapeo_river_mapping'
                )
            """)
            assert not cursor.fetchone()[0]
