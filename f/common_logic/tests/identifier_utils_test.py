from f.common_logic.identifier_utils import (
    camel_to_snake,
    normalize_and_snakecase_keys,
    sanitize_sql_message,
)


def test_sanitize_sql_message():
    message = {
        "col.1": 1,
        "col?2": 2,
        "Cultura/col3": 3,
        "col_002": 4,
        "x": 5,
    }
    global_mapping = {"x": "X"}

    sql_message, updated_global_mapping = sanitize_sql_message(
        message, global_mapping, "/", [("/", "__")]
    )
    assert sql_message == {
        "col1": 1,
        "col2": 2,
        "col3__Cultura": 3,
        "col_002": 4,
        "X": 5,
    }
    assert updated_global_mapping == {
        "x": "X",
        "col.1": "col1",
        "col?2": "col2",
        "Cultura/col3": "col3__Cultura",
        "col_002": "col_002",
    }


def test_sanitize_sql_message__same_letters():
    message = {"foo[bar]test": 1, "foo{bar}test": 2}

    sql_message, _ = sanitize_sql_message(message, {}, maxlen=12)
    assert sql_message == {
        "foobartest": 1,
        "foobarte_001": 2,
    }


def test_sanitize_sql_message__column_names__long():
    message = {
        "column.1": 1,
        "column12": 2,
        "column13": 3,
        "col_002": 4,
        "x": 5,
    }

    sql_message, updated_global_mapping = sanitize_sql_message(message, {}, maxlen=7)
    print(sql_message)
    assert sql_message == {
        "column1": 1,
        "col_001": 2,
        "col_002": 3,  # Note that column13 got assigned another actual column's name!
        "col_003": 4,  # Note that col_002 got renamed to col_003!
        "x": 5,
    }
    assert updated_global_mapping == {
        "column.1": "column1",
        "column12": "col_001",
        "column13": "col_002",  # !!
        "col_002": "col_003",  # !!
        "x": "x",
    }


def test_sanitize_sql_message__with_nesting():
    """sanitize_sql_message() JSON-serializes deeply nested types."""
    message = {
        "group1": {"group2": {"question": "How ya doin?"}},
        "url": "gopher://example.net",
    }

    sql_message, _ = sanitize_sql_message(message, {})
    assert sql_message == {
        "group1": '{"group2": {"question": "How ya doin?"}}',
        "url": "gopher://example.net",
    }


def test_camel_to_snake():
    assert camel_to_snake("CamelCase") == "camel_case"
    assert camel_to_snake("Camel_Case") == "camel_case"
    assert camel_to_snake("camelCase") == "camel_case"
    assert camel_to_snake("camel_case") == "camel_case"
    assert camel_to_snake("camel_case_") == "camel_case_"
    assert camel_to_snake("camel_case_") == "camel_case_"
    assert camel_to_snake("URLLink") == "url_link"
    assert camel_to_snake("URL_Link") == "url_link"


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
