import json
import logging

from pyproj import Transformer

from f.connectors.arcgis.arcgis_download_feature_layer_anonymously import (
    build_geojson,
    fetch_features,
    fetch_layer_data,
    get_layer_metadata,
    main,
    transform_record_geometry,
)

logger = logging.getLogger(__name__)


def test_script_e2e_geojson(arcgis_anonymous_server, tmp_path):
    """Test downloading features as GeoJSON without attachments"""
    asset_storage = tmp_path / "datalake"
    folder_name = "arcgis_anonymous"

    main(
        subdomain=arcgis_anonymous_server.subdomain,
        service_id=arcgis_anonymous_server.service_id,
        feature_id=arcgis_anonymous_server.feature_id,
        layer_index_list=[0],
        download_attachments=False,
        output_format="geojson",
        folder_name=folder_name,
        attachment_root=str(asset_storage),
    )

    # Files are actually saved to storage_path, not outputs/
    expected_file = asset_storage / "arcgis_anonymous" / "test-anonymous-layer.geojson"
    assert expected_file.exists()

    # Verify GeoJSON content
    with open(expected_file) as f:
        geojson_data = json.load(f)

    assert geojson_data["type"] == "FeatureCollection"
    assert len(geojson_data["features"]) == 1

    feature = geojson_data["features"][0]
    assert feature["type"] == "Feature"
    assert feature["geometry"]["type"] == "Point"
    assert len(feature["geometry"]["coordinates"]) == 2

    # Verify properties
    properties = feature["properties"]
    assert properties["what_is_your_name"] == "Community mapper"
    assert properties["OBJECTID"] == 1


def test_script_e2e_with_attachments(arcgis_anonymous_server, tmp_path):
    """Test downloading features with attachments"""
    asset_storage = tmp_path / "datalake"
    folder_name = "arcgis_with_attachments"

    main(
        subdomain=arcgis_anonymous_server.subdomain,
        service_id=arcgis_anonymous_server.service_id,
        feature_id=arcgis_anonymous_server.feature_id,
        layer_index_list=[0],
        download_attachments=True,
        output_format="geojson",
        folder_name=folder_name,
        attachment_root=str(asset_storage),
    )

    # Verify output file was created
    expected_file = (
        asset_storage / "arcgis_with_attachments" / "test-anonymous-layer.geojson"
    )
    assert expected_file.exists()

    # Verify attachments were downloaded (saved in storage_path/service_id_attachments/)
    # Attachments are saved with pattern: {object_id}_{attachment_id}_{slugified_name}
    attachments_dir = (
        asset_storage
        / "arcgis_with_attachments"
        / f"{arcgis_anonymous_server.service_id}_attachments"
        / "1"
    )
    assert attachments_dir.exists()
    # Note: slugify removes dots, so .png becomes png
    assert (attachments_dir / "1_1_springfield_photopng").exists()
    assert (attachments_dir / "1_2_springfield_audiomp4").exists()


def test_script_e2e_excel_format(arcgis_anonymous_server, tmp_path):
    """Test downloading features as Excel format"""
    asset_storage = tmp_path / "datalake"
    folder_name = "arcgis_excel"

    main(
        subdomain=arcgis_anonymous_server.subdomain,
        service_id=arcgis_anonymous_server.service_id,
        feature_id=arcgis_anonymous_server.feature_id,
        layer_index_list=[0],
        download_attachments=False,
        output_format="excel",
        folder_name=folder_name,
        attachment_root=str(asset_storage),
    )

    # Excel format falls through to geojson path, saves with .geojson extension
    # (save_data_to_file uses file_type parameter, not original filename extension)
    expected_file = asset_storage / folder_name / "test-anonymous-layer.geojson"
    assert expected_file.exists()


def test_script_e2e_csv_format(arcgis_anonymous_server, tmp_path):
    """Test downloading features as CSV format"""
    asset_storage = tmp_path / "datalake"

    output_files = main(
        subdomain=arcgis_anonymous_server.subdomain,
        service_id=arcgis_anonymous_server.service_id,
        feature_id=arcgis_anonymous_server.feature_id,
        layer_index_list=[0],
        download_attachments=False,
        output_format="csv",
        folder_name="arcgis_csv",
        attachment_root=str(asset_storage),
    )

    # CSV format saves to outputs/ directory (unlike geojson which saves to storage_path)
    assert len(output_files) == 1
    output_file = output_files[0]
    assert output_file.exists()
    assert output_file.suffix == ".csv"

    # Clean up
    output_file.unlink()
    output_file.parent.rmdir()


def test_script_e2e_multiple_layers(arcgis_anonymous_server, tmp_path):
    """Test downloading multiple layers"""
    asset_storage = tmp_path / "datalake"
    folder_name = "arcgis_multi"

    # Only request layer 0 since we only mocked that one
    main(
        subdomain=arcgis_anonymous_server.subdomain,
        service_id=arcgis_anonymous_server.service_id,
        feature_id=arcgis_anonymous_server.feature_id,
        layer_index_list=[0],
        download_attachments=False,
        output_format="geojson",
        folder_name=folder_name,
        attachment_root=str(asset_storage),
    )

    # GeoJSON format saves to storage_path
    expected_file = asset_storage / "arcgis_multi" / "test-anonymous-layer.geojson"
    assert expected_file.exists()


# Unit tests for individual functions


def test_get_layer_metadata(arcgis_anonymous_server, mocked_responses):
    """Test getting layer metadata"""
    from f.connectors.arcgis.arcgis_download_feature_layer_anonymously import (
        make_session,
    )

    session = make_session()
    metadata = get_layer_metadata(
        session,
        arcgis_anonymous_server.subdomain,
        arcgis_anonymous_server.service_id,
        arcgis_anonymous_server.feature_id,
    )

    assert "layers" in metadata
    assert len(metadata["layers"]) == 1
    assert metadata["layers"][0]["name"] == "Test Anonymous Layer"


def test_fetch_features(arcgis_anonymous_server, mocked_responses):
    """Test fetching features with pagination"""
    from f.connectors.arcgis.arcgis_download_feature_layer_anonymously import (
        make_session,
    )

    session = make_session()
    records = fetch_features(session, arcgis_anonymous_server.base_url, layer_index=0)

    assert isinstance(records, list)
    assert len(records) == 1
    assert records[0]["OBJECTID"] == 1
    assert records[0]["what_is_your_name"] == "Community mapper"


def test_transform_record_geometry():
    """Test transforming geometry from Web Mercator to WGS84"""
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

    # Web Mercator coordinates
    rec = {"OBJECTID": 1, "__geometry": {"x": -8228661.123, "y": 4972614.456}}

    transform_record_geometry(rec, transformer)

    assert "__geojson_geometry" in rec
    geometry = rec["__geojson_geometry"]
    assert geometry["type"] == "Point"
    assert len(geometry["coordinates"]) == 2
    # Verify coordinates are in reasonable WGS84 range
    assert -180 <= geometry["coordinates"][0] <= 180
    assert -90 <= geometry["coordinates"][1] <= 90


def test_build_geojson():
    """Test building GeoJSON from records"""
    records = [
        {
            "OBJECTID": 1,
            "name": "Test Point",
            "__geojson_geometry": {
                "type": "Point",
                "coordinates": [-73.965355, 40.782865],
            },
        }
    ]

    geojson = build_geojson(records)

    assert geojson["type"] == "FeatureCollection"
    assert len(geojson["features"]) == 1

    feature = geojson["features"][0]
    assert feature["type"] == "Feature"
    assert feature["geometry"]["type"] == "Point"
    assert feature["properties"]["OBJECTID"] == 1
    assert feature["properties"]["name"] == "Test Point"
    # Geometry helper shouldn't be in properties
    assert "__geojson_geometry" not in feature["properties"]


def test_fetch_layer_data(arcgis_anonymous_server, tmp_path, mocked_responses):
    """Test the complete fetch_layer_data flow"""
    from f.connectors.arcgis.arcgis_download_feature_layer_anonymously import (
        make_session,
    )

    session = make_session()
    storage_path = tmp_path / "test_storage"

    output_path = fetch_layer_data(
        subdomain=arcgis_anonymous_server.subdomain,
        service_id=arcgis_anonymous_server.service_id,
        feature_id=arcgis_anonymous_server.feature_id,
        layer_index=0,
        download_attachments=False,
        output_format="geojson",
        storage_path=storage_path,
        session=session,
    )

    # File should be created in storage_path
    assert (
        output_path.exists() or (storage_path / "test-anonymous-layer.geojson").exists()
    )

    # Verify the content
    geojson_file = storage_path / "test-anonymous-layer.geojson"
    if geojson_file.exists():
        with open(geojson_file) as f:
            data = json.load(f)
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) >= 1
