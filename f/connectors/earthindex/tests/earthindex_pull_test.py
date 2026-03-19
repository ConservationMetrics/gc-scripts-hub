import json

import psycopg
import requests

from f.connectors.earthindex.earthindex_pull import (
    fetch_layer_points,
    fetch_project,
    format_features_as_geojson,
    main,
)
from f.connectors.earthindex.tests.assets.server_responses import (
    SAMPLE_POINTS,
    SAMPLE_PROJECT,
)


def test_fetch_project(earthindex_server):
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {earthindex_server['api_key']}"})

    project = fetch_project(session, earthindex_server["project_id"])

    assert project["title"] == "Springfield mapping"
    assert project["id"] == SAMPLE_PROJECT["id"]


def test_fetch_layer_points(earthindex_server):
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {earthindex_server['api_key']}"})

    layer_id = SAMPLE_PROJECT["layers"][0]["id"]
    points = fetch_layer_points(session, earthindex_server["project_id"], layer_id)

    assert points["type"] == "FeatureCollection"
    assert len(points["features"]) == len(SAMPLE_POINTS["features"])


def test_format_features_as_geojson():
    project_id = SAMPLE_PROJECT["id"]
    project_title = SAMPLE_PROJECT["title"]
    layer_id = SAMPLE_PROJECT["layers"][0]["id"]

    result = format_features_as_geojson(SAMPLE_POINTS, project_id, project_title, layer_id)

    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == len(SAMPLE_POINTS["features"])

    feature = result["features"][0]
    assert feature["properties"]["layer_id"] == layer_id
    assert feature["properties"]["data_source"] == "Earth Index"
    assert feature["properties"]["project_id"] == project_id
    assert feature["properties"]["project_title"] == "Springfield mapping"
    assert "score" in feature["properties"]
    assert "label" in feature["properties"]


def test_format_features_as_geojson_empty():
    result = format_features_as_geojson(
        {"features": []}, "proj-1", "Empty Project", "layer-1"
    )

    assert result == {"type": "FeatureCollection", "features": []}


def test_script_e2e(earthindex_server, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"

    main(
        earthindex_server["api_key"],
        earthindex_server["project_id"],
        pg_database,
        "earthindex",
        asset_storage,
    )

    # Project JSON is saved to disk
    project_path = asset_storage / "earthindex" / "springfield_mapping" / "project.json"
    assert project_path.exists()
    with open(project_path) as f:
        project_data = json.load(f)
        assert project_data["title"] == "Springfield mapping"

    # GeoJSON is saved to disk
    geojson_path = (
        asset_storage
        / "earthindex"
        / "springfield_mapping"
        / "earthindex_springfield_mapping.geojson"
    )
    assert geojson_path.exists()
    with open(geojson_path) as f:
        geojson_data = json.load(f)
        assert geojson_data["type"] == "FeatureCollection"
        assert len(geojson_data["features"]) == 7

    # Data is written to PostgreSQL
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM earthindex_springfield_mapping")
            assert cursor.fetchone()[0] == 7

            cursor.execute("SELECT * FROM earthindex_springfield_mapping LIMIT 0")
            columns = [desc[0] for desc in cursor.description]

            assert "label" in columns
            assert "score" in columns
            assert "layer_id" in columns
            assert "data_source" in columns
            assert "project_id" in columns
            assert "project_title" in columns
            assert "g__type" in columns
            assert "g__coordinates" in columns

            cursor.execute(
                "SELECT data_source, project_title FROM earthindex_springfield_mapping LIMIT 1"
            )
            row = cursor.fetchone()
            assert row[0] == "Earth Index"
            assert row[1] == "Springfield mapping"

            cursor.execute(
                "SELECT g__type FROM earthindex_springfield_mapping LIMIT 1"
            )
            assert cursor.fetchone()[0] == "Polygon"


def test_no_layers_skips_processing(earthindex_server_no_layers, pg_database, tmp_path):
    asset_storage = tmp_path / "datalake"

    main(
        earthindex_server_no_layers["api_key"],
        earthindex_server_no_layers["project_id"],
        pg_database,
        "earthindex",
        asset_storage,
    )

    # Project JSON is still saved
    project_path = asset_storage / "earthindex" / "springfield_mapping" / "project.json"
    assert project_path.exists()

    # No GeoJSON file created
    geojson_path = (
        asset_storage
        / "earthindex"
        / "springfield_mapping"
        / "earthindex_springfield_mapping.geojson"
    )
    assert not geojson_path.exists()

    # No table created in PostgreSQL
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'earthindex_springfield_mapping'
                )
            """)
            assert not cursor.fetchone()[0]
