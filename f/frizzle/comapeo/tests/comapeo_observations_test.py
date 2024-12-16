import psycopg2

from f.frizzle.comapeo.comapeo_observations import (
    main,
    snakecase_keys_with_collision_handling,
)


def test_snakecase_keys_with_collision_handling():
    input_dict = {
        "camelCaseKey": "value1",
        "anotherCamelCaseKey": "value2",
        "keyWith-Collision": "value3",
        "keyWithCollision": "value4",
        "key_with_collision": "value5",
        "key_with_collision_2": "value6",
    }

    expected_output = {
        "camel_case_key": "value1",
        "another_camel_case_key": "value2",
        "key_with_collision": "value3",
        "key_with_collision_2": "value4",
        "key_with_collision_3": "value5",
        "key_with_collision_2_2": "value6",
    }

    result = snakecase_keys_with_collision_handling(input_dict)

    assert result == expected_output, f"Expected {expected_output}, but got {result}"


def test_script_e2e(comapeoserver, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"

    main(
        comapeoserver.comapeo_server,
        comapeoserver.comapeo_project_blocklist,
        pg_database,
        "comapeo",
        asset_storage,
    )

    # Attachments are saved to disk
    assert (
        asset_storage / "comapeo" / "forest_expedition" / "attachments" / "capybara.jpg"
    ).exists()

    with psycopg2.connect(**pg_database) as conn:
        # Survey responses from forest_expedition are written to a SQL Table in expected format
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM comapeo_forest_expedition")
            assert cursor.fetchone()[0] == 3

            cursor.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'comapeo_forest_expedition'"
            )
            columns = [row[0] for row in cursor.fetchall()]

            assert "notes" in columns

            # Test that the column names are snake_case and handling potential collisions
            assert "created_at_2" in columns

            cursor.execute("SELECT g__type FROM comapeo_forest_expedition LIMIT 1")
            assert cursor.fetchone()[0] == "Point"

        # comapeo_river_mapping SQL Table does not exist (it's in the blocklist)
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'comapeo_river_mapping'
                )
            """)
            assert not cursor.fetchone()[0]
