import psycopg

from f.connectors.csv.csv_to_postgres import main, transform_csv_data

csv_fixture_path = "f/connectors/csv/tests/assets/"


def test_script_e2e(pg_database):
    main(pg_database, "my_csv_data", "data.csv", csv_fixture_path, False)

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM my_csv_data")
            assert cursor.fetchone()[0] == 3

            cursor.execute(
                "SELECT _id, species_name, resource_use, ecosystem_type, availability_status FROM my_csv_data WHERE _id = '1'"
            )
            row_data = cursor.fetchone()
            assert row_data == (
                "1",
                "Palm Tree A",
                "Construction material",
                "Lowland forest",
                "Declining",
            )

            cursor.execute(
                "SELECT _id, species_name, resource_use, ecosystem_type, availability_status FROM my_csv_data WHERE _id = '2'"
            )
            row_data = cursor.fetchone()
            assert row_data == (
                "2",
                "Fish Species B",
                "Primary protein source",
                "River system",
                "Stable",
            )

            cursor.execute(
                "SELECT _id, species_name, resource_use, ecosystem_type, availability_status FROM my_csv_data WHERE _id = '3'"
            )
            row_data = cursor.fetchone()
            assert row_data == (
                "3",
                "Timber Species C",
                "Housing and tools",
                "Mountain forest",
                "Critically low",
            )

            # Check that there is no __columns table created
            cursor.execute(
                "SELECT * FROM information_schema.tables WHERE table_name = 'my_csv_data__columns'"
            )
            assert cursor.fetchone() is None


def test_script_with_custom_id_column(pg_database):
    main(
        pg_database,
        "my_csv_data_custom_id",
        "data_with_id.csv",
        csv_fixture_path,
        False,
        "plot_id",
    )

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM my_csv_data_custom_id")
            assert cursor.fetchone()[0] == 2

            cursor.execute(
                "SELECT _id, land_use_type, resource_density FROM my_csv_data_custom_id WHERE _id = 'PLT001'"
            )
            row_data = cursor.fetchone()
            assert row_data == (
                "PLT001",
                "Agroforestry system",
                "High productivity",
            )

            cursor.execute(
                "SELECT _id, land_use_type, resource_density FROM my_csv_data_custom_id WHERE _id = 'PLT002'"
            )
            row_data = cursor.fetchone()
            assert row_data == (
                "PLT002",
                "Selective logging area",
                "Moderate depletion",
            )


def test_transform_csv_data__empty_cells_become_none(tmp_path):
    """Empty CSV cells round-trip to ``None`` so the DB can store NULL.

    This matches typical CSV conventions and preserves NULL semantics when a
    CSV produced by ``save_data_to_file`` (which writes ``None`` as an empty
    cell) is read back in.
    """
    csv_file = tmp_path / "sparse.csv"
    csv_file.write_text("_id,name,note\n1,Alpha,\n2,,Bravo note\n")

    rows = transform_csv_data(csv_file, id_column="_id")
    assert rows == [
        {"_id": "1", "name": "Alpha", "note": None},
        {"_id": "2", "name": None, "note": "Bravo note"},
    ]


def test_script_with_mapping_table_and_key_reversal(pg_database, tmp_path):
    """Forwarding use_mapping_table/reverse_properties_separated_by to the
    StructuredDBWriter enables form-style ingestion (used by ODK/Kobo/Epi)."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(
        '"_id","meta/instanceID","name"\n'
        '"abc","uuid:111","Alpha"\n'
        '"def","uuid:222","Bravo"\n'
    )

    main(
        pg_database,
        "form_responses",
        "data.csv",
        str(tmp_path),
        False,
        "_id",
        use_mapping_table=True,
        reverse_properties_separated_by="/",
    )

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM form_responses")
            assert cursor.fetchone()[0] == 2

            # meta/instanceID got reversed to instanceID__meta
            cursor.execute(
                'SELECT "instanceID__meta" FROM form_responses WHERE _id = \'abc\''
            )
            assert cursor.fetchone() == ("uuid:111",)

            # Mapping table is created and recorded
            cursor.execute(
                "SELECT sql_column FROM form_responses__columns "
                "WHERE original_column = 'meta/instanceID'"
            )
            assert cursor.fetchone() == ("instanceID__meta",)
