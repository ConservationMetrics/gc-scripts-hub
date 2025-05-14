import psycopg2

from f.connectors.kobotoolbox.kobotoolbox_responses import main


def test_script_e2e(koboserver, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "kobo_responses"

    main(
        koboserver.account,
        koboserver.form_id,
        pg_database,
        table_name,
        asset_storage,
    )

    # Attachments are saved to disk
    assert (asset_storage / table_name / "attachments" / "1637241249813.jpg").exists()

    # Metadata is saved to disk
    assert (asset_storage / table_name / f"{table_name}_metadata.json").exists()
    with open(asset_storage / table_name / f"{table_name}_metadata.json") as f:
        metadata = f.read()
    assert all(
        key in metadata for key in ["name", "uid", "owner__username", "data", "content"]
    )

    # Survey responses are written to a SQL Table
    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cursor.fetchone()[0] == 3

            # Check that the coordinates of a fixture entry are stored as a Point,
            # and that the coordinates are reversed (longitude, latitude).
            cursor.execute(
                f"SELECT g__type, g__coordinates FROM {table_name} WHERE _id = '124961136'"
            )
            assert cursor.fetchone() == ("Point", "[-122.0109429, 36.97012]")

            # Check that meta/instanceID was sanitized to instanceID__meta
            cursor.execute(
                f"SELECT \"instanceID__meta\" FROM {table_name} WHERE _id = '124961136'"
            )
            assert cursor.fetchone() == ("uuid:e58da38d-3eee-4bd7-8512-4a97ea8fbb01",)

            # Check that the mapping column was created
            cursor.execute(
                f"SELECT COUNT(*) FROM {table_name}__columns WHERE original_column = 'meta/instanceID' AND sql_column = 'instanceID__meta'"
            )
            assert cursor.fetchone()[0] == 1

    # Form labels are written to a SQL Table
    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}__labels")
            assert cursor.fetchone()[0] == 8

            # Verify specific translations for survey items
            cursor.execute(
                f"SELECT label_en, label_es, label_pt FROM {table_name}__labels WHERE name = 'Record_your_current_location'"
            )
            assert cursor.fetchone() == (
                "Record your current location",
                "Registre la ubicación actual",
                "Registre a localização atual",
            )

            cursor.execute(
                f"SELECT label_en, label_es, label_pt FROM {table_name}__labels WHERE name = 'Estimate_height_of_your_tree_in_meters'"
            )
            assert cursor.fetchone() == (
                "Estimate the height of your tree (in meters)",
                "Estime la altura de su árbol (en metros)",
                "Estime a altura da sua árvore (em metros)",
            )

            # Verify specific translations for choice items
            cursor.execute(
                f"SELECT label_en, label_es, label_pt FROM {table_name}__labels WHERE name = 'shade'"
            )
            assert cursor.fetchone() == ("Shade", "Sombra", "Sombra")

            cursor.execute(
                f"SELECT label_en, label_es, label_pt FROM {table_name}__labels WHERE name = 'wildlife_habitat'"
            )
            assert cursor.fetchone() == (
                "Wildlife Habitat",
                "Hábitat de vida silvestre",
                "Habitat da vida selvagem",
            )

            # Check that the type is set for survey / choice items
            cursor.execute(
                f"SELECT type FROM {table_name}__labels WHERE name = 'Record_your_current_location'"
            )
            assert cursor.fetchone() == ("survey",)
            cursor.execute(
                f"SELECT type FROM {table_name}__labels WHERE name = 'shade'"
            )
            assert cursor.fetchone() == ("choices",)


def test_script_e2e__no_translations(koboserver_no_translations, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"
    table_name = "kobo_no_translations"

    main(
        koboserver_no_translations.account,
        koboserver_no_translations.form_id,
        pg_database,
        table_name,
        asset_storage,
    )

    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            # Confirm that for the labels table, there is only a labels column, no language suffix
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}__labels")
            assert cursor.fetchone()[0] == 8
            cursor.execute(
                f"SELECT label FROM {table_name}__labels WHERE name = 'Record_your_current_location'"
            )
            assert cursor.fetchone() == ("Record your current location",)
