from f.export.postgres_to_geojson.postgres_to_geojson import main
import json


def test_script_e2e(pg_database, tmp_path):
    asset_storage = tmp_path / "datalake/export"

    main(
        pg_database,
        "comapeo_data",
        asset_storage,
    )

    with open(asset_storage / "comapeo_data.geojson") as f:
        data = json.load(f)

        assert data["type"] == "FeatureCollection"
        assert isinstance(data["features"], list)
        for feature in data["features"]:
            assert feature["type"] == "Feature"
            assert "geometry" in feature
            assert "properties" in feature

        assert data["features"][0]["id"] == "doc_id_1"
        assert data["features"][0]["geometry"]["coordinates"] == "[151.2093, -33.8688]"
        assert data["features"][0]["properties"]["project_name"] == "Forest Expedition"
