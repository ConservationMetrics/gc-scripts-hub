import psycopg2

from f.connectors.geojson.geojson_to_postgres import main

geojson_fixture_path = "f/connectors/geojson/tests/assets/"


def test_script_e2e(pg_database):
    main(pg_database, "my_geojson_data", "data.geojson", geojson_fixture_path, False)

    with psycopg2.connect(**pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM my_geojson_data")
            assert cursor.fetchone()[0] == 3

            cursor.execute(
                "SELECT g__type, g__coordinates, name, height, age, species FROM my_geojson_data WHERE _id = '1'"
            )
            point_data = cursor.fetchone()
            assert point_data == (
                "Point",
                "[-105.01621, 39.57422]",
                "Pine Tree",
                "30",
                "50",
                "Pinus ponderosa",
            )

            cursor.execute(
                "SELECT g__type, g__coordinates, name, length, flow_rate, water_type FROM my_geojson_data WHERE _id = '2'"
            )
            line_data = cursor.fetchone()
            assert line_data == (
                "LineString",
                "[[-105.01621, 39.57422], [-105.01621, 39.57423], [-105.01622, 39.57424]]",
                "River Stream",
                "2.5",
                "moderate",
                "freshwater",
            )

            cursor.execute(
                "SELECT g__type, g__coordinates, name, area, flora, fauna FROM my_geojson_data WHERE _id = '3'"
            )
            polygon_data = cursor.fetchone()
            assert polygon_data == (
                "Polygon",
                "[[[-105.01621, 39.57422], [-105.01621, 39.57423], [-105.01622, 39.57423], [-105.01622, 39.57422], [-105.01621, 39.57422]]]",
                "Meadow",
                "1.2",
                '["wildflowers", "grasses"]',
                '["deer", "rabbits"]',
            )

            # Check that there is no __columns table created
            cursor.execute(
                "SELECT * FROM information_schema.tables WHERE table_name = 'my_geojson_data__columns'"
            )
            assert cursor.fetchone() is None
