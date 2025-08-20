from f.common_logic.identifier_utils import (
    camel_to_snake,
    normalize_and_snakecase_keys,
    normalize_identifier,
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
    """Test camel_to_snake function focusing on edge cases and regex behavior."""

    # Core regex pattern testing - cases that aren't covered by normalize_identifier
    assert camel_to_snake("") == ""
    assert camel_to_snake("a") == "a"
    assert camel_to_snake("A") == "a"
    assert camel_to_snake("1") == "1"
    assert camel_to_snake("123") == "123"

    # Leading/trailing underscores preserved (unique to camel_to_snake)
    assert camel_to_snake("_CamelCase") == "_camel_case"
    assert camel_to_snake("CamelCase_") == "camel_case_"
    assert camel_to_snake("_CamelCase_") == "_camel_case_"

    # Complex uppercase sequence transitions (XMLHttp pattern)
    assert camel_to_snake("XMLHttpRequest") == "xml_http_request"
    assert camel_to_snake("HTTPSConnection") == "https_connection"


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


def test_normalize_identifier_default_params():
    """Test default parameters and core functionality."""
    # Test special case: _id preservation
    assert normalize_identifier("_id") == "_id"

    # Test basic transformations
    assert normalize_identifier("kebab-case") == "kebab_case"
    assert normalize_identifier("123project") == "_123project"
    assert normalize_identifier("") == "_"
    assert normalize_identifier("!@#$%") == "_"
    assert normalize_identifier("___name___") == "name"
    assert normalize_identifier("This is my dataset, ok?") == "this_is_my_dataset_ok"
    assert normalize_identifier("Foo bar baz") == "foo_bar_baz"
    assert normalize_identifier("Summary of results (Q1)") == "summary_of_results_q1"
    assert normalize_identifier("2024 field survey data") == "_2024_field_survey_data"
    assert normalize_identifier("Location 1 / Sector B") == "location_1___sector_b"
    assert normalize_identifier("Foo bar's dataset!") == "foo_bars_dataset"
    assert normalize_identifier("Table: Foo Bar 2") == "table_foo_bar_2"
    assert normalize_identifier("Results - Phase 1") == "results___phase_1"
    assert (
        normalize_identifier(
            "this is a very, very, very long dataset name that will get truncated safely"
        )
        == "this_is_a_very_very_very_long_dataset_name_that_will_get_trunca"
    )


def test_normalize_identifier_maxlen_param():
    """Test maxlen parameter with various length strings."""
    # Test default maxlen (63)
    long_string = "this_is_a_very_long_identifier_name_that_exceeds_default_limit_test"
    result = normalize_identifier(long_string)
    assert len(result) <= 63
    assert result == "this_is_a_very_long_identifier_name_that_exceeds_default_limit_"

    # Test custom maxlen
    assert normalize_identifier("hello_world", maxlen=5) == "hello"
    assert normalize_identifier("test", maxlen=10) == "test"
    assert normalize_identifier("a", maxlen=1) == "a"

    # Test maxlen=0 edge case
    assert normalize_identifier("test", maxlen=0) == ""


def test_normalize_identifier_make_snake_param():
    """Test make_snake parameter for CamelCase conversion."""
    # Test with make_snake=True (default) - just one example
    assert normalize_identifier("CamelCaseString") == "camel_case_string"

    # Test with make_snake=False - focus on the parameter behavior
    assert (
        normalize_identifier("CamelCaseString", make_snake=False) == "CamelCaseString"
    )
    assert normalize_identifier("XMLHttpRequest", make_snake=False) == "XMLHttpRequest"

    # Test interaction with special characters - this is unique to normalize_identifier
    assert (
        normalize_identifier("Camel-Case.String", make_snake=False)
        == "Camel_Case_String"
    )


def test_normalize_identifier_ensure_leading_alpha_param():
    """Test ensure_leading_alpha parameter."""
    # Test with ensure_leading_alpha=True (default)
    assert normalize_identifier("123name") == "_123name"
    assert normalize_identifier("") == "_"
    assert normalize_identifier("!@#$%") == "_"

    # Test with ensure_leading_alpha=False
    assert normalize_identifier("123name", ensure_leading_alpha=False) == "123name"
    assert normalize_identifier("", ensure_leading_alpha=False) == "_"
    assert normalize_identifier("!@#$%", ensure_leading_alpha=False) == "_"

    # Test strings that already start with alpha - should be unchanged
    assert normalize_identifier("valid_name") == "valid_name"
    assert normalize_identifier("_underscore") == "underscore"


def test_normalize_identifier_sep_policy_param():
    """Test sep_policy parameter for separator handling."""
    # Test with sep_policy="underscore" (default)
    assert normalize_identifier("hello world") == "hello_world"
    assert normalize_identifier("file-name") == "file_name"
    assert normalize_identifier("path/to/file") == "path_to_file"
    assert normalize_identifier("data.table") == "data_table"
    assert (
        normalize_identifier("mixed-name with_spaces/and.dots")
        == "mixed_name_with_spaces_and_dots"
    )

    # Test with sep_policy="remove"
    assert normalize_identifier("hello world", sep_policy="remove") == "helloworld"
    assert normalize_identifier("file-name", sep_policy="remove") == "filename"
    assert normalize_identifier("path/to/file", sep_policy="remove") == "pathtofile"
    assert normalize_identifier("data.table", sep_policy="remove") == "datatable"
    assert (
        normalize_identifier("mixed-name with_spaces/and.dots", sep_policy="remove")
        == "mixednamewith_spacesanddots"
    )

    # Test with multiple consecutive separators
    assert (
        normalize_identifier("hello   world", sep_policy="underscore")
        == "hello___world"
    )
    assert (
        normalize_identifier("hello---world", sep_policy="underscore")
        == "hello___world"
    )
    assert (
        normalize_identifier("hello...world", sep_policy="underscore")
        == "hello___world"
    )
    assert normalize_identifier("hello   world", sep_policy="remove") == "helloworld"
    assert normalize_identifier("hello---world", sep_policy="remove") == "helloworld"


def test_normalize_identifier_unicode_handling():
    """Test Unicode character handling and accent removal."""
    # Test various accent types
    assert normalize_identifier("Vigilância Ambiental") == "vigilancia_ambiental"
    assert normalize_identifier("naïve café") == "naive_cafe"
    assert normalize_identifier("résumé") == "resume"
    assert normalize_identifier("Müller") == "muller"


def test_normalize_identifier_complex_combinations():
    """Test complex combinations of all parameters."""
    # Test combination: accents + maxlen + snake_case
    long_accented = "VigilânciaAmbientalDaRegiãoMetropolitana"
    result = normalize_identifier(long_accented, maxlen=25)
    assert result == "vigilancia_ambiental_da_r"
    assert len(result) <= 25

    # Test combination: numeric start + separators + policy interaction
    numeric_mixed = "123-my file.name"
    result = normalize_identifier(
        numeric_mixed, make_snake=False, ensure_leading_alpha=True, sep_policy="remove"
    )
    assert result == "_123myfilename"


def test_normalize_identifier_edge_cases():
    """Test edge cases and boundary conditions."""
    # Empty string
    assert normalize_identifier("") == "_"
    assert normalize_identifier("", ensure_leading_alpha=False) == "_"

    # Only special characters
    assert normalize_identifier("!@#$%^&*()") == "_"
    assert normalize_identifier("!@#$%^&*()") == "_"

    # Only separators
    assert normalize_identifier("---...///") == "_"
    assert normalize_identifier("   ", sep_policy="remove") == "_"

    # Only underscores
    assert normalize_identifier("___") == "_"
    assert normalize_identifier("___name___") == "name"

    # Very short maxlen
    assert normalize_identifier("test", maxlen=1) == "t"
    assert normalize_identifier("test", maxlen=2) == "te"

    # Maxlen with ensure_leading_alpha
    assert normalize_identifier("123", maxlen=2, ensure_leading_alpha=True) == "_1"
    assert normalize_identifier("123", maxlen=1, ensure_leading_alpha=True) == "_"
