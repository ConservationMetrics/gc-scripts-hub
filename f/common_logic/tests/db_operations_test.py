import pytest
import testing.postgresql

from f.common_logic.db_operations import StructuredDBWriter, conninfo


@pytest.fixture
def mock_db_connection():
    """A dsn that may be used to connect to a live (local for test) postgresql server"""
    db = testing.postgresql.Postgresql(port=7654)
    dsn = db.dsn()
    dsn["dbname"] = dsn.pop("database")
    yield conninfo(dsn)
    db.stop()


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
