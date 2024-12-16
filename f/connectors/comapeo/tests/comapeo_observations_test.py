import psycopg2

from f.connectors.comapeo.comapeo_observations import (
    main,
    normalize_and_snakecase_keys,
)


def test_normalize_and_snakecase_keys():
    input_dict = {
        "primaryKey": 1,
        "camelCaseKey": 2,
        "anotherCamelCaseKey": 3,
        "keyWith-Collision": 4,
        "keyWithCollision": 5,
        "KeyWithCollision": 6,
        "key-with-collision": 7,
        "key_with_collision": 8,
        "key_with_collision_2": 9,
        "aVeryLongKeyNameThatExceedsTheSixtyThreeCharacterLimitAndNeedsTruncation": 10,
        "aVeryLongKeyNameThatExceedsTheSixtyThreeCharacterLimitAndNeedsTruncationAlso": 11,
    }

    special_case_keys = set(["primaryKey"])

    expected_output = {
        "primaryKey": 1,
        "camel_case_key": 2,
        "another_camel_case_key": 3,
        "key_with_collision": 4,
        "key_with_collision_2": 5,
        "key_with_collision_3": 6,
        "key_with_collision_4": 7,
        "key_with_collision_5": 8,
        "key_with_collision_2_2": 9,
        "a_very_long_key_name_that_exceeds_the_sixty_three_character_l_1": 10,
        "a_very_long_key_name_that_exceeds_the_sixty_three_character_l_2": 11,
    }

    result = normalize_and_snakecase_keys(input_dict, special_case_keys)

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

            cursor.execute("SELECT * FROM comapeo_forest_expedition LIMIT 0")
            columns = [desc[0] for desc in cursor.description]

            assert "notes" in columns

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
