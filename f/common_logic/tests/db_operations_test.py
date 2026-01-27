from f.common_logic.db_operations import (
    StructuredDBWriter,
    check_if_table_exists,
    conninfo,
    fetch_tables_from_postgres,
    summarize_new_rows_updates_and_columns,
)


def test_fetch_tables_from_postgres(mock_db_connection):
    table_names = ["test_forms", "test_forms_2"]
    writers = [StructuredDBWriter(mock_db_connection, name) for name in table_names]

    with writers[0]._get_conn() as conn, conn.cursor() as cursor:
        for writer in writers:
            cursor.execute(
                f"CREATE TABLE {writer.table_name} (_id VARCHAR PRIMARY KEY, field VARCHAR)"
            )

    tables = fetch_tables_from_postgres(mock_db_connection)

    assert isinstance(tables, list)
    for writer in writers:
        assert writer.table_name in tables


def test_check_if_table_exists(mock_db_connection):
    writer = StructuredDBWriter(mock_db_connection, "existing_table")

    submissions = [
        {"_id": "1", "field": "value"},
    ]
    writer.handle_output(submissions)

    assert check_if_table_exists(mock_db_connection, "existing_table")
    assert not check_if_table_exists(mock_db_connection, "nonexistent_table")


def test_mapping_table_creation(mock_db_connection):
    writer = StructuredDBWriter(
        mock_db_connection, "test_forms", use_mapping_table=True
    )

    submissions = [
        {"_id": "1", "complex.field": "value1"},
        {"_id": "2", "complex.field": "value2"},
    ]
    writer.handle_output(submissions)

    # Check the mapping table contains expected mappings
    mapping_table = f"{writer.table_name}__columns"
    mappings = writer._get_existing_mappings(mapping_table)
    assert "complex.field" in mappings
    assert mappings["complex.field"] == "complexfield"


def test_no_mapping_table_creation(mock_db_connection):
    writer = StructuredDBWriter(mock_db_connection, "test_forms")

    submissions = [
        {"_id": "1", "complex.field": "value1"},
        {"_id": "2", "complex.field": "value2"},
    ]
    writer.handle_output(submissions)

    # Check the mapping table does not exist
    with writer._get_conn() as conn, conn.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM information_schema.tables WHERE table_name = %s",
            (f"{writer.table_name}__columns",),
        )
        assert cursor.fetchone() is None

    # Check that the fields were sanitized
    columns = writer._inspect_schema(writer.table_name)
    assert "complexfield" in columns


def test_reverse_properties_handling(mock_db_connection):
    writer = StructuredDBWriter(
        mock_db_connection,
        "nested_data",
        reverse_properties_separated_by="/",
        str_replace=[("/", "__")],
    )

    submissions = [
        {"_id": "1", "group1/subgroup/field": "value", "group2/field": "value2"}
    ]
    writer.handle_output(submissions)

    # Check the table columns reflect reversed and transformed paths
    columns = writer._inspect_schema("nested_data")
    assert "field__subgroup__group1" in columns
    assert "field__group2" in columns


def test_long_table_name_truncation(mock_db_connection):
    very_long_name = "this_is_an_extremely_long_table_name_that_exceeds_postgresql_limits_significantly_2023"
    writer = StructuredDBWriter(
        mock_db_connection, very_long_name, use_mapping_table=True
    )

    # Verify both main and mapping table names are properly truncated
    assert len(writer.table_name) == 63
    assert (
        writer.table_name
        == "this_is_an_extremely_long_table_name_that_exceeds_postgresql_li"
    )
    mapping_table = "this_is_an_extremely_long_table_name_that_exceeds_post__columns"
    assert len(mapping_table) == 63

    # Verify tables can be created with truncated names
    submissions = [{"_id": "1", "test": "value"}]
    writer.handle_output(submissions)

    # Verify both tables exist and are accessible
    assert writer._inspect_schema(writer.table_name)
    assert writer._inspect_schema(mapping_table)


def test_truncated_table_name_retains_suffix(mock_db_connection):
    very_long_name = (
        "this_is_an_extremely_long_table_name_that_should_truncate_properly"
    )
    suffix = "labels"
    writer = StructuredDBWriter(
        mock_db_connection,
        very_long_name,
        suffix=suffix,
        use_mapping_table=False,
    )

    # Table name should end with __labels and be max 63 chars
    assert writer.table_name.endswith(f"__{suffix}")
    assert len(writer.table_name) <= 63

    # Actual table should be creatable and inspectable
    submissions = [{"_id": "1", "field": "value"}]
    writer.handle_output(submissions)
    assert writer._inspect_schema(writer.table_name)


def test_table_name_normalization_to_lowercase(mock_db_connection):
    writer = StructuredDBWriter(
        mock_db_connection,
        "ALL_CAPS_TABLE_NAME",
        use_mapping_table=True,
    )

    submissions = [{"_id": "1", "key": "value"}]
    writer.handle_output(submissions)

    assert writer.table_name == "all_caps_table_name"

    mapping_table = f"{writer.table_name}__columns"
    assert mapping_table == "all_caps_table_name__columns"

    with writer._get_conn() as conn, conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
            (writer.table_name,),
        )
        assert cursor.fetchone() is not None

        cursor.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
            (mapping_table,),
        )
        assert cursor.fetchone() is not None


def test_summarize_all_new_rows(mock_db_dict):
    """Test detection of all new rows when table exists but has different data"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "test_dataset")

    # Create existing data
    existing_data = [
        {"_id": "1", "name": "Alice", "age": "30"},
        {"_id": "2", "name": "Bob", "age": "25"},
    ]
    writer.handle_output(existing_data)

    # New data with different IDs (all new rows)
    new_data = [
        {"_id": "3", "name": "Charlie", "age": "35"},
        {"_id": "4", "name": "Diana", "age": "28"},
        {"_id": "5", "name": "Eve", "age": "32"},
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "test_dataset", new_data
    )

    assert new_rows == 3
    assert updates == 0
    assert new_columns == 0


def test_summarize_all_updates(mock_db_dict):
    """Test detection of updates when all rows have existing IDs but changed values"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "test_dataset")

    # Create existing data
    existing_data = [
        {"_id": "1", "name": "Alice", "age": "30"},
        {"_id": "2", "name": "Bob", "age": "25"},
    ]
    writer.handle_output(existing_data)

    # Same IDs but different values (all updates)
    new_data = [
        {"_id": "1", "name": "Alice", "age": "31"},  # age changed
        {"_id": "2", "name": "Robert", "age": "25"},  # name changed
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "test_dataset", new_data
    )

    assert new_rows == 0
    assert updates == 2
    assert new_columns == 0


def test_summarize_new_columns(mock_db_dict):
    """Test detection of new columns in the new data"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "test_dataset")

    # Create existing data with basic columns
    existing_data = [
        {"_id": "1", "name": "Alice"},
        {"_id": "2", "name": "Bob"},
    ]
    writer.handle_output(existing_data)

    # New data with additional columns
    new_data = [
        {"_id": "3", "name": "Charlie", "age": "35", "city": "NYC"},
        {"_id": "4", "name": "Diana", "age": "28", "country": "USA"},
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "test_dataset", new_data
    )

    assert new_rows == 2
    assert updates == 0
    assert new_columns == 3  # age, city, country


def test_summarize_mixed_scenario(mock_db_dict):
    """Test mixed scenario with new rows, updates, and new columns"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "test_dataset")

    # Create existing data
    existing_data = [
        {"_id": "1", "name": "Alice", "age": "30"},
        {"_id": "2", "name": "Bob", "age": "25"},
        {"_id": "3", "name": "Charlie", "age": "35"},
    ]
    writer.handle_output(existing_data)

    # Mixed: 1 update, 2 new rows, 1 new column
    new_data = [
        {"_id": "1", "name": "Alice", "age": "31", "city": "NYC"},  # update + new col
        {"_id": "4", "name": "Diana", "age": "28"},  # new row
        {"_id": "5", "name": "Eve", "age": "32", "city": "LA"},  # new row + new col
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "test_dataset", new_data
    )

    assert new_rows == 2  # IDs 4 and 5
    assert updates == 1  # ID 1 has changed age
    assert new_columns == 1  # city


def test_summarize_no_changes(mock_db_dict):
    """Test when new data is identical to existing data"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "test_dataset")

    # Create existing data
    existing_data = [
        {"_id": "1", "name": "Alice", "age": "30"},
        {"_id": "2", "name": "Bob", "age": "25"},
    ]
    writer.handle_output(existing_data)

    # Identical data (no changes)
    new_data = [
        {"_id": "1", "name": "Alice", "age": "30"},
        {"_id": "2", "name": "Bob", "age": "25"},
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "test_dataset", new_data
    )

    assert new_rows == 0
    assert updates == 0
    assert new_columns == 0


def test_summarize_empty_new_data(mock_db_dict):
    """Test with empty new data list"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "test_dataset")

    # Create existing data
    existing_data = [{"_id": "1", "name": "Alice"}]
    writer.handle_output(existing_data)

    # Empty new data
    new_data = []

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "test_dataset", new_data
    )

    assert new_rows == 0
    assert updates == 0
    assert new_columns == 0


def test_summarize_null_value_handling(mock_db_dict):
    """Test proper handling of None/NULL values in comparisons"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "test_dataset")

    # Create existing data with None value
    existing_data = [
        {"_id": "1", "name": "Alice", "age": None},
        {"_id": "2", "name": "Bob", "age": "25"},
    ]
    writer.handle_output(existing_data)

    # New data with None values and changes
    new_data = [
        {"_id": "1", "name": "Alice", "age": None},  # No change (None == None)
        {"_id": "2", "name": "Bob", "age": None},  # Update (25 -> None)
        {"_id": "3", "name": "Charlie", "age": None},  # New row with None
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "test_dataset", new_data
    )

    assert new_rows == 1  # ID 3
    assert updates == 1  # ID 2 changed from 25 to None
    assert new_columns == 0


def test_summarize_partial_columns_in_new_data(mock_db_dict):
    """Test when new data has fewer columns than existing table"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "test_dataset")

    # Create existing data with more columns
    existing_data = [
        {"_id": "1", "name": "Alice", "age": "30", "city": "NYC"},
        {"_id": "2", "name": "Bob", "age": "25", "city": "LA"},
    ]
    writer.handle_output(existing_data)

    # New data with fewer columns
    new_data = [
        {"_id": "1", "name": "Alice"},  # Missing age and city
        {"_id": "3", "name": "Charlie"},  # New row with partial data
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "test_dataset", new_data
    )

    assert new_rows == 1  # ID 3
    # ID 1 should not be counted as update since we only compare common columns
    assert updates == 0
    assert new_columns == 0


def test_summarize_string_number_comparison(mock_db_dict):
    """Test that string and numeric values are properly compared"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "test_dataset")

    # Create existing data (stored as strings in TEXT columns)
    existing_data = [
        {"_id": "1", "value": "42"},
        {"_id": "2", "value": "100"},
    ]
    writer.handle_output(existing_data)

    # New data that looks identical but might have type differences
    new_data = [
        {"_id": "1", "value": "42"},  # Same
        {"_id": "2", "value": "101"},  # Changed
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "test_dataset", new_data
    )

    assert new_rows == 0
    assert updates == 1  # ID 2 changed from 100 to 101
    assert new_columns == 0


def test_summarize_custom_primary_key(mock_db_dict):
    """Test using a custom primary key column instead of _id"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "test_dataset")

    # Create existing data with custom primary key
    existing_data = [
        {"_id": "uuid1", "email": "alice@example.com", "name": "Alice"},
        {"_id": "uuid2", "email": "bob@example.com", "name": "Bob"},
    ]
    writer.handle_output(existing_data)

    # New data using email as primary key for comparison
    new_data = [
        {"email": "alice@example.com", "name": "Alice Updated"},  # Update
        {"email": "charlie@example.com", "name": "Charlie"},  # New
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "test_dataset", new_data, primary_key="email"
    )

    assert new_rows == 1  # charlie@example.com
    assert updates == 1  # alice@example.com has changed name
    assert new_columns == 0


def test_summarize_kobotoolbox_style_ids(mock_db_dict):
    """Test scenario matching the user's KoboToolbox CSV with large numeric IDs"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "kobotoolbox_submissions")

    # Simulate initial upload with 3 KoboToolbox submissions
    existing_data = [
        {
            "_id": "254135872",
            "community": "Arlington",
            "state": "Flourishing",
            "practices": "Daily",
        },
        {
            "_id": "254136591",
            "community": "Springfield",
            "state": "Declining",
            "practices": "Daily",
        },
        {
            "_id": "254136930",
            "community": "Burke",
            "state": "Stable",
            "practices": "Monthly",
        },
    ]
    writer.handle_output(existing_data)

    # Simulate re-upload with same 3 rows + 2 new rows (SAME columns)
    new_upload_data = [
        {
            "_id": "254135872",
            "community": "Arlington",
            "state": "Flourishing",
            "practices": "Daily",
        },
        {
            "_id": "254136591",
            "community": "Springfield",
            "state": "Declining",
            "practices": "Daily",
        },
        {
            "_id": "254136930",
            "community": "Burke",
            "state": "Stable",
            "practices": "Monthly",
        },
        {
            "_id": "254136939",
            "community": "Frederick",
            "state": "Stable",
            "practices": "Monthly",
        },  # NEW
        {
            "_id": "254136999",
            "community": "Occoquan",
            "state": "Stable",
            "practices": "Monthly",
        },  # NEW
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "kobotoolbox_submissions", new_upload_data
    )

    assert new_rows == 2  # Frederick and Occoquan
    assert updates == 0  # No existing rows changed
    assert new_columns == 0  # Same columns - this is the key assertion!


def test_summarize_with_new_columns_added(mock_db_dict):
    """Test detecting new columns when schema evolves"""
    writer = StructuredDBWriter(conninfo(mock_db_dict), "evolving_dataset")

    # Initial upload with basic columns
    existing_data = [
        {"_id": "1", "name": "Alice", "age": "30"},
        {"_id": "2", "name": "Bob", "age": "25"},
    ]
    writer.handle_output(existing_data)

    # Re-upload with additional columns
    new_upload_data = [
        {"_id": "1", "name": "Alice", "age": "30", "city": "NYC", "country": "USA"},
        {"_id": "2", "name": "Bob", "age": "25", "city": "LA", "country": "USA"},
        {
            "_id": "3",
            "name": "Charlie",
            "age": "35",
            "city": "Chicago",
            "country": "USA",
        },
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "evolving_dataset", new_upload_data
    )

    assert new_rows == 1  # Charlie
    assert updates == 0  # Alice and Bob have same values in existing columns
    assert new_columns == 2  # city and country are new


def test_summarize_reupload_same_csv_all_columns(mock_db_dict):
    """
    Test re-uploading the EXACT same CSV structure - should detect 0 new columns.
    This simulates the user's scenario where they upload the same CSV twice.
    """
    writer = StructuredDBWriter(conninfo(mock_db_dict), "kobo_full_schema")

    # Simulate full KoboToolbox CSV with ALL columns from first upload
    # (mimicking all 46+ columns from the actual CSV)
    full_columns_row1 = {
        "_id": "254135872",
        "start": "2023-07-19 14:30:12",
        "end": "2023-07-19 14:33:05",
        "today": "2023-07-18",
        "community_question": "Arlington",
        "ecosystem_state": "Flourishing",
        "plants_animals": "bamboo, wild boar",
        "harvesting_frequency": "Daily",
        "threat_deforestation": "",
        "threat_mining": "",
        "threat_logging": "",
        "threat_pollution": "",
        # Add ~40 more columns to simulate real KoboToolbox CSV
        "col1": "val1",
        "col2": "val2",
        "col3": "val3",
        "col4": "val4",
        "col5": "val5",
        "col6": "val6",
        "col7": "val7",
        "col8": "val8",
        "col9": "val9",
        "col10": "val10",
        "col11": "val11",
        "col12": "val12",
        "col13": "val13",
        "col14": "val14",
        "col15": "val15",
        "col16": "val16",
        "col17": "val17",
        "col18": "val18",
        "col19": "val19",
        "col20": "val20",
        "col21": "val21",
        "col22": "val22",
        "col23": "val23",
        "col24": "val24",
        "col25": "val25",
        "col26": "val26",
        "col27": "val27",
        "col28": "val28",
        "col29": "val29",
        "col30": "val30",
        "col31": "val31",
        "col32": "val32",
        "col33": "val33",
        "col34": "val34",
        "col35": "val35",
        "col36": "val36",
    }

    full_columns_row2 = {
        **full_columns_row1,
        "_id": "254136591",
        "community_question": "Springfield",
    }
    full_columns_row3 = {
        **full_columns_row1,
        "_id": "254136930",
        "community_question": "Burke",
    }

    # Initial upload with 3 rows - ALL columns present
    initial_data = [full_columns_row1, full_columns_row2, full_columns_row3]
    writer.handle_output(initial_data)

    # Re-upload with 5 rows - SAME columns, 2 new rows
    full_columns_row4 = {
        **full_columns_row1,
        "_id": "254136939",
        "community_question": "Frederick",
    }
    full_columns_row5 = {
        **full_columns_row1,
        "_id": "254136999",
        "community_question": "Occoquan",
    }

    reupload_data = [
        full_columns_row1,
        full_columns_row2,
        full_columns_row3,
        full_columns_row4,
        full_columns_row5,
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "kobo_full_schema", reupload_data
    )

    assert new_rows == 2  # Frederick and Occoquan
    assert updates == 0  # First 3 rows unchanged
    assert new_columns == 0  # NO new columns - this is what the user expects!


def test_summarize_new_dataset_without_existing_table(mock_db_dict):
    """Test that we can count rows and columns even when table doesn't exist"""
    # Don't create any table - simulate uploading to a new dataset

    # Simulate new data being uploaded
    new_data = [
        {"_id": "1", "name": "Alice", "age": "30", "city": "NYC"},
        {"_id": "2", "name": "Bob", "age": "25", "city": "LA"},
        {"_id": "3", "name": "Charlie", "age": "35"},  # Missing city column
    ]

    # This should handle the case where table doesn't exist gracefully
    # by returning 0,0,0 (since table doesn't exist, no comparison is possible)
    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "nonexistent_table", new_data
    )

    # When table doesn't exist, the function currently returns based on
    # whether the primary key exists in the table columns
    # Since table doesn't exist, it should handle this gracefully
    assert new_rows == 3  # All rows are new since table doesn't exist
    assert updates == 0
    assert new_columns >= 3  # At least _id, name, age (city might not be in all rows)


def test_summarize_actual_kobotoolbox_csv_reupload(mock_db_dict):
    """
    Test using the ACTUAL column names from the user's KoboToolbox CSV.
    This will verify that sanitize_sql_message produces the same column names
    that StructuredDBWriter creates in the database.
    """
    writer = StructuredDBWriter(conninfo(mock_db_dict), "kobotoolbox_test")

    # Use ACTUAL column names from the user's CSV (first 3 rows)
    initial_row1 = {
        "_id": "254135872",
        "start": "2023-07-19 14:30:12.239000-04:00",
        "end": "2023-07-19 14:33:05.995000-04:00",
        "today": "2023-07-18",
        "What community are you from?": "Arlington",
        "Enter the community name:": "",
        "How do you describe the current state of the local ecosystem?": "Flourishing",
        "Which traditional plants and animals are important to your community?": "bamboo, wild boar",
        "How frequently do you engage in traditional harvesting practices?": "Daily",
        "What are the primary threats to the biodiversity in your area?": "Mining",
        "What are the primary threats to the biodiversity in your area?/Deforestation": "",
        "What are the primary threats to the biodiversity in your area?/Mining": "",
        "What are the primary threats to the biodiversity in your area?/Logging": "",
        "What are the primary threats to the biodiversity in your area?/Pollution": "",
        "What are the primary threats to the biodiversity in your area?/Climate Change": "",
        "What are the primary threats to the biodiversity in your area?/Overharvesting": "",
        "What are the primary threats to the biodiversity in your area?/Land encroachment": "",
        "What are the primary threats to the biodiversity in your area?/Other": "",
        "Please specify the other threat:": "",
        "Are there any changes in the timing or behavior of migratory species you have observed?": "Yes",
        "How would you rate the availability of traditional resources over the past decade?": "Stable",
        "Have you noticed any changes in the abundance or health of key indicator species?": "No",
        "How is traditional ecological knowledge (TEK) passed on within your community?": "Oral storytelling",
        "How is traditional ecological knowledge (TEK) passed on within your community?/Oral storytelling": "",
        "How is traditional ecological knowledge (TEK) passed on within your community?/Apprenticeships": "",
        "How is traditional ecological knowledge (TEK) passed on within your community?/Elders' guidance": "",
        "How is traditional ecological knowledge (TEK) passed on within your community?/Rituals and ceremonies": "",
        "How is traditional ecological knowledge (TEK) passed on within your community?/Schools/educational institutions": "",
        "How is traditional ecological knowledge (TEK) passed on within your community?/Other": "",
        "_uuid": "92eb7237-89d6-4a15-aa71-09ec99279bbb",
        "_submission_time": "2023-07-19 18:33:06",
        "_status": "submitted_via_web",
    }

    initial_row2 = {
        **initial_row1,
        "_id": "254136591",
        "What community are you from?": "Springfield",
    }
    initial_row3 = {
        **initial_row1,
        "_id": "254136930",
        "What community are you from?": "Burke",
    }

    # Initial upload with 3 rows
    initial_data = [initial_row1, initial_row2, initial_row3]
    writer.handle_output(initial_data)

    # Re-upload with same 3 rows + 2 new rows (EXACT same columns)
    reupload_row4 = {
        **initial_row1,
        "_id": "254136939",
        "What community are you from?": "Frederick",
    }
    reupload_row5 = {
        **initial_row1,
        "_id": "254136999",
        "What community are you from?": "Occoquan",
    }

    reupload_data = [
        initial_row1,
        initial_row2,
        initial_row3,
        reupload_row4,
        reupload_row5,
    ]

    new_rows, updates, new_columns = summarize_new_rows_updates_and_columns(
        mock_db_dict, "kobotoolbox_test", reupload_data
    )

    assert new_rows == 2  # Frederick and Occoquan
    assert updates == 0  # First 3 rows unchanged
    assert new_columns == 0  # NO new columns - CRITICAL TEST!
