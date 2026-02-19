import psycopg

from f.connectors.arcgis.arcgis_feature_layer import (
    main,
)


def test_script_e2e(arcgis_server, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"

    main(
        arcgis_server.account,
        arcgis_server.feature_layer_url,
        pg_database,
        "my_arcgis_data",
        asset_storage,
    )

    # Attachments are saved to disk
    assert (
        asset_storage / "my_arcgis_data" / "attachments" / "springfield_photo.png"
    ).exists()

    with psycopg.connect(autocommit=True, **pg_database) as conn:
        # Survey responses from arcgis_feature_layer are written to a SQL Table in expected format
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM my_arcgis_data")
            assert cursor.fetchone()[0] == 1

            cursor.execute("SELECT * FROM my_arcgis_data LIMIT 0")
            columns = [desc[0] for desc in cursor.description]

            assert "what_is_your_name" in columns

            assert "what_is_the_date_and_time" in columns

            assert "add_a_photo_filename" in columns

            assert "add_an_audio_content_type" in columns

            cursor.execute("SELECT g__type FROM my_arcgis_data LIMIT 1")
            assert cursor.fetchone()[0] == "Point"

            cursor.execute("SELECT g__coordinates FROM my_arcgis_data LIMIT 1")
            assert cursor.fetchone()[0] == "[-73.965355, 40.782865]"
