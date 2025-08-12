import json
import tempfile
from pathlib import Path

import psycopg2

from f.connectors.geojson.geojson_to_postgres import main, transform_geojson_data

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


def test_transform_geojson_data_with_missing_ids():
    """Test that features without IDs get auto-generated UUIDs."""
    # Create a temporary GeoJSON file without IDs
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
                "properties": {"name": "Test Point"},
            },
            {
                "type": "Feature",
                "id": "existing_id",
                "geometry": {"type": "Point", "coordinates": [3.0, 4.0]},
                "properties": {"name": "Point with ID"},
            },
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
        json.dump(geojson_data, f)
        temp_path = f.name

    try:
        # Transform the data
        transformed_data = transform_geojson_data(temp_path)

        # Check that we have 2 features
        assert len(transformed_data) == 2

        # Check that both features have _id fields
        assert all("_id" in feature for feature in transformed_data)

        # Check that existing ID is preserved
        existing_id_feature = next(
            f for f in transformed_data if f["_id"] == "existing_id"
        )
        assert existing_id_feature["name"] == "Point with ID"

        # Check that auto-generated ID is a valid UUID string
        auto_generated_feature = next(
            f for f in transformed_data if f["_id"] != "existing_id"
        )
        assert auto_generated_feature["name"] == "Test Point"
        assert len(auto_generated_feature["_id"]) == 36  # UUID length
        assert auto_generated_feature["_id"].count("-") == 4  # UUID format

    finally:
        # Clean up
        Path(temp_path).unlink()


def test_transform_geojson_data_all_missing_ids():
    """Test that all features without IDs get unique auto-generated UUIDs."""
    # Create a temporary GeoJSON file with no IDs
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [i, i]},
                "properties": {"name": f"Point {i}"},
            }
            for i in range(3)
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
        json.dump(geojson_data, f)
        temp_path = f.name

    try:
        # Transform the data
        transformed_data = transform_geojson_data(temp_path)

        # Check that we have 3 features
        assert len(transformed_data) == 3

        # Check that all features have unique _id fields
        ids = [feature["_id"] for feature in transformed_data]
        assert len(set(ids)) == 3  # All IDs should be unique

        # Check that all IDs are valid UUID strings
        for feature_id in ids:
            assert len(feature_id) == 36  # UUID length
            assert feature_id.count("-") == 4  # UUID format

    finally:
        # Clean up
        Path(temp_path).unlink()


def test_transform_geojson_data_deterministic_ids():
    """Test that deterministic IDs are generated consistently for the same content."""
    # Create a temporary GeoJSON file with no IDs
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
                "properties": {"name": "Test Point", "value": 42},
            }
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
        json.dump(geojson_data, f)
        temp_path = f.name

    try:
        # Transform the data twice
        transformed_data_1 = transform_geojson_data(temp_path)
        transformed_data_2 = transform_geojson_data(temp_path)

        # Check that the same feature generates the same ID both times
        assert len(transformed_data_1) == 1
        assert len(transformed_data_2) == 1
        assert transformed_data_1[0]["_id"] == transformed_data_2[0]["_id"]

        # Check that the ID is a valid UUID string
        feature_id = transformed_data_1[0]["_id"]
        assert len(feature_id) == 36  # UUID length
        assert feature_id.count("-") == 4  # UUID format

    finally:
        # Clean up
        Path(temp_path).unlink()
