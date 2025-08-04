from f.common_logic.db_transformations import (
    camel_to_snake,
    force_valid_sql_name,
    sanitize,
)


def test_sanitize():
    message = {
        "col.1": 1,
        "col?2": 2,
        "Cultura/col3": 3,
        "col_002": 4,
        "x": 5,
    }
    global_mapping = {"x": "X"}

    sql_message, updated_global_mapping = sanitize(
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


def test_sanitize__same_letters():
    message = {"foo[bar]test": 1, "foo{bar}test": 2}

    sql_message, _ = sanitize(message, {}, maxlen=12)
    assert sql_message == {
        "foobartest": 1,
        "foobarte_001": 2,
    }


def test_sanitize_column_names__long():
    message = {
        "column.1": 1,
        "column12": 2,
        "column13": 3,
        "col_002": 4,
        "x": 5,
    }

    sql_message, updated_global_mapping = sanitize(message, {}, maxlen=7)
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


def test_sanitize_with_nesting():
    """sanitize() JSON-serializes deeply nested types."""
    message = {
        "group1": {"group2": {"question": "How ya doin?"}},
        "url": "gopher://example.net",
    }

    sql_message, _ = sanitize(message, {})
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


def test_force_valid_sql_name():
    assert force_valid_sql_name("This is my dataset, ok?") == "This_is_my_dataset_ok"
    assert force_valid_sql_name("Foo bar baz") == "Foo_bar_baz"
    assert force_valid_sql_name("Summary of results (Q1)") == "Summary_of_results_Q1"
    assert force_valid_sql_name("2024 field survey data") == "_2024_field_survey_data"
    assert force_valid_sql_name("Location 1 / Sector B") == "Location_1___Sector_B"
    assert force_valid_sql_name("Foo bar's dataset!") == "Foo_bars_dataset"
    assert force_valid_sql_name("Table: Foo Bar 2") == "Table_Foo_Bar_2"
    assert force_valid_sql_name("Results - Phase 1") == "Results___Phase_1"
    assert (
        force_valid_sql_name(
            "this is a very long dataset name that will get truncated safely"
        )
        == "this_is_a_very_long_dataset_name_that_will_get_truncated_safel"
    )
    assert force_valid_sql_name("Foo bar's dataset!") == "Foo_bars_dataset"
    assert force_valid_sql_name("Table: Foo Bar 2") == "Table_Foo_Bar_2"
