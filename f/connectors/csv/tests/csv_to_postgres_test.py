import psycopg

from f.connectors.csv.csv_to_postgres import main

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
