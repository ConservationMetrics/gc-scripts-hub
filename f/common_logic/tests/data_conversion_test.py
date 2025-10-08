import pytest

from f.common_logic.data_conversion import convert_data, slugify


def _validate_geojson_structure(result, expected_feature_count):
    """Helper to validate basic GeoJSON structure."""
    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == expected_feature_count

    # Ensure all features have IDs (required for database insertion)
    for i, feature in enumerate(result["features"]):
        assert "id" in feature, f"Feature {i} missing required 'id' field"
        assert feature["id"], f"Feature {i} has empty 'id' field"


def _validate_point_geometry(feature):
    """Helper to validate point geometry structure."""
    geometry = feature["geometry"]
    properties = feature["properties"]

    # Ensure geometry is a plain dict, not a fiona.Geometry object
    assert isinstance(geometry, dict)
    assert not hasattr(geometry, "__geo_interface__")

    assert geometry["type"] == "Point"
    assert isinstance(geometry["coordinates"], (list, tuple))
    assert len(geometry["coordinates"]) >= 2  # lon, lat (may have elevation)
    assert isinstance(properties, dict)


def _validate_coordinates_in_bounds(coords, lon_bounds, lat_bounds):
    """Helper to validate coordinates are within specified bounds."""
    assert lon_bounds[0] < coords[0] < lon_bounds[1], (
        f"Longitude {coords[0]} not in range {lon_bounds}"
    )
    assert lat_bounds[0] < coords[1] < lat_bounds[1], (
        f"Latitude {coords[1]} not in range {lat_bounds}"
    )


def _assert_osmand_property(props, key, expected_value=None):
    """Helper to check OsmAnd properties with or without namespace prefix."""
    has_property = f"osmand:{key}" in props or key in props
    assert has_property, f"Missing OsmAnd property: {key}"

    if expected_value is not None:
        actual_value = props.get(f"osmand:{key}") or props.get(key)
        assert actual_value == expected_value, (
            f"Expected {key}={expected_value}, got {actual_value}"
        )


def test_convert_data__locusmap_points_gpx(locusmap_points_gpx_file):
    result, output_format = convert_data(str(locusmap_points_gpx_file), "gpx")
    assert output_format == "geojson"
    _validate_geojson_structure(result, len(result["features"]))  # At least 2 waypoints
    assert len(result["features"]) >= 2

    # Filter to waypoints with descriptions (the main features we care about)
    waypoints_with_desc = [
        f
        for f in result["features"]
        if f["geometry"]["type"] == "Point" and "desc" in f["properties"]
    ]
    assert len(waypoints_with_desc) >= 2

    for feature in waypoints_with_desc:
        _validate_point_geometry(feature)
        properties = feature["properties"]

        assert "desc" in properties
        assert any(word in properties["desc"].lower() for word in ["tree", "rock"])

        assert "link" in properties
        assert any(properties["link"].strip().endswith(ext) for ext in [".jpg", ".m4a"])


def test_convert_data__locusmap_points_kml(locusmap_points_kml_file):
    result, output_format = convert_data(str(locusmap_points_kml_file), "kml")
    assert output_format == "geojson"
    _validate_geojson_structure(result, 2)

    for feat in result["features"]:
        _validate_point_geometry(feat)
        props = feat["properties"]
        assert "name" in props
        assert "description" in props
        assert "attachments" in props


def test_convert_data__locusmap_tracks_kml(locusmap_tracks_kml_file):
    result, output_format = convert_data(str(locusmap_tracks_kml_file), "kml")
    assert output_format == "geojson"

    # Root-level structure check
    _validate_geojson_structure(result, 4)

    # Group by geometry type (accept both LineString and MultiLineString)
    lines = [
        f
        for f in result["features"]
        if f["geometry"]["type"] in ["LineString", "MultiLineString"]
    ]
    points = [f for f in result["features"] if f["geometry"]["type"] == "Point"]

    assert len(lines) == 3
    assert len(points) == 1

    # Validate each track
    for line in lines:
        geom = line["geometry"]
        props = line["properties"]

        # Coordinates sanity check (handle both LineString and MultiLineString)
        assert isinstance(geom["coordinates"], list)
        if geom["type"] == "LineString":
            assert len(geom["coordinates"]) > 1
        elif geom["type"] == "MultiLineString":
            assert len(geom["coordinates"]) > 0
            for linestring in geom["coordinates"]:
                assert len(linestring) > 1
                assert all(
                    isinstance(coord, (list, tuple)) and len(coord) >= 2
                    for coord in linestring
                )

        if geom["type"] == "LineString":
            assert all(
                isinstance(coord, (list, tuple)) and len(coord) >= 2
                for coord in geom["coordinates"]
            )

        # Metadata expectations
        assert "name" in props
        assert props["name"].startswith("2025-01-17")
        assert "description" in props
        assert any(
            keyword in props["description"].lower()
            for keyword in ["walk", "trees", "path"]
        )

    # Check the single point feature
    pt = points[0]
    _validate_point_geometry(pt)
    props = pt["properties"]

    assert "attachments" in props
    assert props["attachments"].endswith(".jpg")
    assert "description" in props
    assert "willow" in props["description"].lower()


def test_convert_data__locusmap_tracks_gpx(locusmap_tracks_gpx_file):
    result, output_format = convert_data(str(locusmap_tracks_gpx_file), "gpx")
    assert output_format == "geojson"

    # Root-level sanity checks
    _validate_geojson_structure(result, len(result["features"]))
    assert len(result["features"]) > 4

    # Split into geometry types
    lines = [
        f
        for f in result["features"]
        if f["geometry"]["type"] in ["LineString", "MultiLineString"]
    ]
    points = [f for f in result["features"] if f["geometry"]["type"] == "Point"]

    assert len(lines) >= 3  # At least 3 track features
    assert len(points) >= 1  # At least 1 waypoint

    # Validate track features (look for named tracks, not individual track points)
    named_tracks = [
        f for f in lines if f["properties"].get("name", "").startswith("2025-01-17")
    ]
    assert len(named_tracks) >= 3

    for track in named_tracks:
        coords = track["geometry"]["coordinates"]
        props = track["properties"]

        # Coordinates validation (handle both LineString and MultiLineString)
        if track["geometry"]["type"] == "MultiLineString":
            assert isinstance(coords, list)
            assert all(isinstance(line, list) for line in coords)
        else:
            assert isinstance(coords, list)
            assert len(coords) > 1

        # Required track metadata
        assert "name" in props
        assert props["name"].startswith("2025-01-17")

        if "desc" in props:
            assert any(
                word in props["desc"].lower() for word in ["walk", "path", "tree"]
            )

    # Validate waypoint features (look for features with descriptions about willow)
    waypoints_with_desc = [p for p in points if "desc" in p["properties"]]
    assert len(waypoints_with_desc) >= 1

    willow_point = next(
        (p for p in waypoints_with_desc if "willow" in p["properties"]["desc"].lower()),
        None,
    )
    assert willow_point is not None


def test_convert_data__garmin_sample_gpx(garmin_sample_gpx_file):
    result, output_format = convert_data(str(garmin_sample_gpx_file), "gpx")
    assert output_format == "geojson"
    _validate_geojson_structure(result, len(result["features"]))
    assert len(result["features"]) > 2

    points = [f for f in result["features"] if f["geometry"]["type"] == "Point"]
    lines = [
        f
        for f in result["features"]
        if f["geometry"]["type"] in ["LineString", "MultiLineString"]
    ]

    # Should have waypoints and track features
    assert len(points) >= 1
    assert len(lines) >= 1

    # Waypoint check - look for the White House waypoint
    white_house_point = next(
        (p for p in points if p["properties"].get("name") == "White House"), None
    )
    assert white_house_point is not None
    _validate_point_geometry(white_house_point)
    coords = white_house_point["geometry"]["coordinates"]
    assert coords[0] == -77.03656 and coords[1] == 38.897957
    assert "desc" in white_house_point["properties"]
    assert "start point" in white_house_point["properties"]["desc"].lower()

    # Track check - look for the Track Log
    track_log = next(
        (line for line in lines if line["properties"].get("name") == "Track Log"), None
    )
    assert track_log is not None

    coords = track_log["geometry"]["coordinates"]
    assert isinstance(coords, list)
    if track_log["geometry"]["type"] == "MultiLineString":
        assert all(isinstance(line, list) for line in coords)
    else:
        assert len(coords) >= 2


def test_read_data__kobotoolbox_csv(kobotoolbox_csv_file):
    result, output_format = convert_data(str(kobotoolbox_csv_file), "csv")
    assert output_format == "csv"

    headers = result[0]
    assert "What community are you from?" in headers
    assert "_id" in headers
    assert "_submission_time" in headers

    assert len(result) == 4

    record = result[1]
    assert "Arlington" in record
    assert "Flourishing" in record
    assert "bamboo, wild boar" in record


def test_read_data__csv_only_headers(tmp_path):
    file = tmp_path / "only_headers.csv"
    file.write_text("start,location,comment\n")
    with pytest.raises(ValueError, match="no data"):
        convert_data(str(file), "csv")


def test_convert_data__kobotoolbox_xlsx(kobotoolbox_excel_file):
    result, output_format = convert_data(str(kobotoolbox_excel_file), "xlsx")
    assert output_format == "csv"
    headers = result[0]
    assert "What community are you from?" in headers
    assert "_id" in headers
    assert "_submission_time" in headers

    assert len(result) == 4

    record = result[1]
    assert "Arlington" in record
    assert "Flourishing" in record
    assert "bamboo, wild boar" in record


def test_convert_data__kobotoolbox_multiple_sheets_xlsx(
    kobotoolbox_multiple_sheets_excel_file,
):
    with pytest.raises(ValueError, match="only single-sheet files are supported"):
        convert_data(str(kobotoolbox_multiple_sheets_excel_file), "xlsx")


def test_convert_data__kobotoolbox_empty_csv(kobotoolbox_empty_submission_csv_file):
    with pytest.raises(ValueError, match="no data"):
        convert_data(str(kobotoolbox_empty_submission_csv_file), "csv")


def test_convert_data__json(tmp_path):
    file = tmp_path / "test.json"
    file.write_text('[{"a": 1, "b": 2}, {"a": 3}]')
    result, output_format = convert_data(str(file), "json")
    assert output_format == "csv"
    assert result == [["a", "b"], ["1", "2"], ["3", ""]]


def test_convert_data__json_empty(tmp_path):
    file = tmp_path / "test_empty.json"
    file.write_text("[]")
    with pytest.raises(ValueError, match="JSON file contains no records"):
        convert_data(str(file), "json")


def test_read_data__mapeo_geojson(mapeo_geojson_file):
    result, output_format = convert_data(str(mapeo_geojson_file), "geojson")
    assert output_format == "geojson"
    _validate_geojson_structure(result, 3)

    for feature in result["features"]:
        assert feature["type"] == "Feature"
        assert "geometry" in feature
        assert "properties" in feature
        assert isinstance(feature["properties"], dict)
        assert "type" in feature["properties"]
        assert "notes" in feature["properties"]


def test_convert_data__osm_overpass_gpx(osm_overpass_gpx_file):
    """Test conversion of OSM Overpass GPX data with waypoints."""
    result, output_format = convert_data(str(osm_overpass_gpx_file), "gpx")
    assert output_format == "geojson"

    # Root-level structure validation
    _validate_geojson_structure(result, 15)

    # All features should be waypoints (Points)
    points = [f for f in result["features"] if f["geometry"]["type"] == "Point"]
    assert len(points) == 15

    # Validate geometry and basic structure
    for feature in result["features"]:
        _validate_point_geometry(feature)

    # Validate specific known features from OSM data
    bus_station = next(
        (
            f
            for f in result["features"]
            if "Bus Station Lijn 5" in f["properties"].get("name", "")
        ),
        None,
    )
    assert bus_station is not None
    assert "desc" in bus_station["properties"]
    assert "amenity=bus_station" in bus_station["properties"]["desc"]
    assert "link" in bus_station["properties"]
    assert "osm.org" in bus_station["properties"]["link"]

    # Check restaurant feature
    restaurant = next(
        (
            f
            for f in result["features"]
            if "Soeng Nige's" in f["properties"].get("name", "")
        ),
        None,
    )
    assert restaurant is not None
    assert "amenity=restaurant" in restaurant["properties"]["desc"]

    # Check exchange bureau feature
    exchange = next(
        (
            f
            for f in result["features"]
            if "HJ De Vries Exchange" in f["properties"].get("name", "")
        ),
        None,
    )
    assert exchange is not None
    assert "amenity=bureau_de_change" in exchange["properties"]["desc"]
    assert "Waterkant" in exchange["properties"]["desc"]


def test_convert_data__osm_overpass_geojson(osm_overpass_geojson_file):
    """Test reading of OSM Overpass GeoJSON data."""
    result, output_format = convert_data(str(osm_overpass_geojson_file), "geojson")
    assert output_format == "geojson"

    # Root-level structure validation
    _validate_geojson_structure(result, 15)

    # All features should be Points in this dataset
    for feature in result["features"]:
        assert feature["type"] == "Feature"
        assert feature["geometry"]["type"] == "Point"
        assert isinstance(feature["geometry"]["coordinates"], list)
        assert len(feature["geometry"]["coordinates"]) == 2  # lon, lat
        assert isinstance(feature["properties"], dict)

    # Validate specific OSM features and their properties
    bus_station = next(
        (
            f
            for f in result["features"]
            if f["properties"].get("name") == "Bus Station Lijn 5 bus"
        ),
        None,
    )
    assert bus_station is not None
    props = bus_station["properties"]
    assert props["amenity"] == "bus_station"
    assert props["bus"] == "yes"
    assert props["public_transport"] == "station"
    assert "@id" in props
    assert props["@id"] == "node/1660255196"

    # Check multilingual name support
    assert props["name:en"] == "Line 5 bus station"
    assert props["name:nl"] == "Lijn 5 bus station"

    # Check restaurant with source attribution
    restaurant = next(
        (
            f
            for f in result["features"]
            if f["properties"].get("name") == "Soeng Nige's"
        ),
        None,
    )
    assert restaurant is not None
    assert restaurant["properties"]["amenity"] == "restaurant"
    assert restaurant["properties"]["source"] == "KG Ground Survey 2016"

    # Check address information
    exchange = next(
        (
            f
            for f in result["features"]
            if "HJ De Vries Exchange" in f["properties"].get("name", "")
        ),
        None,
    )
    assert exchange is not None
    props = exchange["properties"]
    assert props["addr:city"] == "Paramaribo"
    assert props["addr:housenumber"] == "92 - 96"
    assert props["addr:street"] == "Waterkant"

    # Check modern POI with crypto payment support
    crypto_shop = next(
        (
            f
            for f in result["features"]
            if "SMG Schaafijs" in f["properties"].get("name", "")
        ),
        None,
    )
    assert crypto_shop is not None
    props = crypto_shop["properties"]
    assert props["amenity"] == "ice_cream"
    assert props["currency:XBT"] == "yes"
    assert props["payment:lightning"] == "yes"
    assert "2024-10-04" in props["survey:date"]


def test_convert_data__osm_overpass_kml(osm_overpass_kml_file):
    """Test conversion of OSM Overpass KML data with ExtendedData."""
    result, output_format = convert_data(str(osm_overpass_kml_file), "kml")
    assert output_format == "geojson"

    # Root-level structure validation
    _validate_geojson_structure(result, 15)

    # All features should be Points in this dataset
    for feature in result["features"]:
        assert feature["type"] == "Feature"
        _validate_point_geometry(feature)

    # Validate KML ExtendedData preservation
    bus_station = next(
        (
            f
            for f in result["features"]
            if f["properties"].get("name") == "Bus Station Lijn 5 bus"
        ),
        None,
    )
    assert bus_station is not None
    props = bus_station["properties"]

    # Check that ExtendedData fields are preserved (these come from XML parsing)
    assert props["@id"] == "node/1660255196"
    assert props["amenity"] == "bus_station"
    assert props["bus"] == "yes"
    assert props["public_transport"] == "station"
    assert props["name:en"] == "Line 5 bus station"
    assert props["name:nl"] == "Lijn 5 bus station"

    # Check restaurant with complete data
    restaurant = next(
        (
            f
            for f in result["features"]
            if f["properties"].get("name") == "Soeng Nige's"
        ),
        None,
    )
    assert restaurant is not None
    assert restaurant["properties"]["amenity"] == "restaurant"
    assert restaurant["properties"]["source"] == "KG Ground Survey 2016"

    # Check address data preservation
    exchange = next(
        (
            f
            for f in result["features"]
            if "HJ De Vries Exchange" in f["properties"].get("name", "")
        ),
        None,
    )
    assert exchange is not None
    props = exchange["properties"]
    assert props["addr:city"] == "Paramaribo"
    assert props["addr:housenumber"] == "92 - 96"
    assert props["addr:street"] == "Waterkant"
    assert props["amenity"] == "bureau_de_change"

    # Check feature with description but no name (name extraction from XML)
    parking_features = [
        f for f in result["features"] if f["properties"].get("amenity") == "parking"
    ]
    assert len(parking_features) == 2

    # Check that one parking has operator info
    harevey_parking = next(
        (
            f
            for f in parking_features
            if "Harevey" in f["properties"].get("operator", "")
        ),
        None,
    )
    assert harevey_parking is not None
    assert harevey_parking["properties"]["operator:type"] == "government"
    assert harevey_parking["properties"]["survey:date"] == "2024-09-18"

    # Check modern feature with crypto payments and description
    crypto_shop = next(
        (
            f
            for f in result["features"]
            if "SMG Schaafijs" in f["properties"].get("name", "")
        ),
        None,
    )
    assert crypto_shop is not None
    props = crypto_shop["properties"]
    assert props["name"] == "SMG Schaafijs and More"
    assert props["description"] == "Shaved ice with variety of flavors and cocktails"
    assert props["amenity"] == "ice_cream"
    assert props["currency:XBT"] == "yes"
    assert props["payment:lightning"] == "yes"
    assert props["payment:lightning_contactless"] == "yes"


def test_osm_data_consistency_across_formats(
    osm_overpass_gpx_file, osm_overpass_geojson_file, osm_overpass_kml_file
):
    """Test that the same OSM data is consistent across GPX, GeoJSON, and KML formats."""
    gpx_result, gpx_format = convert_data(str(osm_overpass_gpx_file), "gpx")
    geojson_result, geojson_format = convert_data(
        str(osm_overpass_geojson_file), "geojson"
    )
    kml_result, kml_format = convert_data(str(osm_overpass_kml_file), "kml")

    assert gpx_format == "geojson"
    assert geojson_format == "geojson"
    assert kml_format == "geojson"

    # All should have the same number of features
    assert len(gpx_result["features"]) == 15
    assert len(geojson_result["features"]) == 15
    assert len(kml_result["features"]) == 15

    # Extract coordinates for comparison (normalize to [lon, lat])
    def get_coordinates(features):
        coords = []
        for f in features:
            geom_coords = f["geometry"]["coordinates"]
            coords.append((geom_coords[0], geom_coords[1]))  # lon, lat only
        return sorted(coords)

    gpx_coords = get_coordinates(gpx_result["features"])
    geojson_coords = get_coordinates(geojson_result["features"])
    kml_coords = get_coordinates(kml_result["features"])

    # All formats should have the same coordinate sets
    assert gpx_coords == geojson_coords == kml_coords

    # Check that key POI names are present in all formats
    def get_feature_names(features):
        names = set()
        for f in features:
            name = f["properties"].get("name")
            if name:
                names.add(name)
        return names

    gpx_names = get_feature_names(gpx_result["features"])
    geojson_names = get_feature_names(geojson_result["features"])
    kml_names = get_feature_names(kml_result["features"])

    # Core names should be present across formats
    expected_names = {
        "Bus Station Lijn 5 bus",
        "Soeng Nige's",
        "HJ De Vries Exchange N.V.",
        "Sara's",
        "SMG Schaafijs and More",
    }

    assert expected_names.issubset(gpx_names)
    assert expected_names.issubset(geojson_names)
    assert expected_names.issubset(kml_names)

    # Verify that amenity data is preserved across formats where available
    def find_feature_by_name(features, name):
        return next((f for f in features if f["properties"].get("name") == name), None)

    # Check bus station across all formats
    gpx_bus = find_feature_by_name(gpx_result["features"], "Bus Station Lijn 5 bus")
    geojson_bus = find_feature_by_name(
        geojson_result["features"], "Bus Station Lijn 5 bus"
    )
    kml_bus = find_feature_by_name(kml_result["features"], "Bus Station Lijn 5 bus")

    assert gpx_bus and geojson_bus and kml_bus

    # GPX has description field, GeoJSON/KML have structured amenity field
    assert "amenity=bus_station" in gpx_bus["properties"]["desc"]
    assert geojson_bus["properties"]["amenity"] == "bus_station"
    assert kml_bus["properties"]["amenity"] == "bus_station"


def test_convert_data__empty_geojson(empty_geojson_file):
    with pytest.raises(ValueError, match="GeoJSON contains no features"):
        convert_data(str(empty_geojson_file), "geojson")


def test_convert_data__geojson_with_missing_properties(
    geojson_with_missing_properties_file,
):
    with pytest.raises(ValueError, match="missing properties"):
        convert_data(str(geojson_with_missing_properties_file), "geojson")


def test_convert_data__geojson_with_invalid_geometry(
    geojson_with_invalid_geometry_file,
):
    with pytest.raises(ValueError, match="invalid geometry coordinates"):
        convert_data(str(geojson_with_invalid_geometry_file), "geojson")


def test_convert_data__geojson_with_invalid_top_level(
    geojson_with_invalid_top_level_structure_file,
):
    with pytest.raises(ValueError, match="must be a FeatureCollection object"):
        convert_data(str(geojson_with_invalid_top_level_structure_file), "geojson")


def test_convert_data__googleearth_sample_kml(googleearth_sample_kml_file):
    result, output_format = convert_data(str(googleearth_sample_kml_file), "kml")
    assert output_format == "geojson"
    _validate_geojson_structure(result, 3)

    for feat in result["features"]:
        geom = feat["geometry"]
        props = feat["properties"]

        assert geom["type"] in [
            "Point",
            "LineString",
            "Polygon",
        ]
        assert isinstance(geom["coordinates"], (list, tuple))

        if geom["type"] == "Point":
            assert (
                len(geom["coordinates"]) >= 2
            )  # basic sanity: lon, lat (may have elevation)
        elif geom["type"] == "LineString":
            assert (
                len(geom["coordinates"]) >= 2
            )  # basic sanity: at least 2 coords for a line
        elif geom["type"] == "Polygon":
            assert (
                len(geom["coordinates"][0]) >= 3
            )  # basic sanity: at least 3 coords for a polygon

        assert "name" in props

    sample = next(
        f
        for f in result["features"]
        if f["properties"].get("name") == "Floating placemark"
    )
    props = sample["properties"]

    assert props["description"] == "Floats a defined distance above the ground."
    assert props["visibility"] == "0"
    assert props["styleUrl"] == "#downArrowIcon"
    assert "lookat_longitude" in props
    assert "lookat_heading" in props
    assert "lookat_tilt" in props


def test_convert_data__gc_alerts_kml(alerts_kml_file):
    result, output_format = convert_data(str(alerts_kml_file), "kml")
    assert output_format == "geojson"
    _validate_geojson_structure(result, 2)

    for feat in result["features"]:
        geom = feat["geometry"]
        props = feat["properties"]

        assert geom["type"] == "Polygon"
        assert isinstance(geom["coordinates"], list)
        assert len(geom["coordinates"][0]) >= 3  # basic sanity: at least 3 coords

        assert "territory" in props
        assert "alertType" in props
        assert "alertID" in props
        assert "t0_url" in props


def test_convert_data__kml_missing_geometry(kml_with_missing_geometry_file):
    with pytest.raises(ValueError, match="No valid features found in input file"):
        convert_data(str(kml_with_missing_geometry_file), "kml")


def test_convert_data__unsupported():
    with pytest.raises(ValueError):
        convert_data("/fake/path.foo", "foo")


def test_convert_data__osmand_notes_gpx(osmand_notes_gpx_file):
    """Test conversion of OsmAnd notes GPX data with photo attachments."""
    result, output_format = convert_data(str(osmand_notes_gpx_file), "gpx")
    assert output_format == "geojson"

    # Root-level structure validation
    _validate_geojson_structure(result, 10)

    # All features should be waypoints (Points)
    points = [f for f in result["features"] if f["geometry"]["type"] == "Point"]
    assert len(points) == 10

    # Validate geometry and basic structure
    for feature in result["features"]:
        _validate_point_geometry(feature)

    # Validate specific OsmAnd photo note features
    photo_notes = [
        f for f in result["features"] if f["properties"].get("type") == "photonote"
    ]
    assert len(photo_notes) == 10

    # Check that all photo notes have required properties
    for note in photo_notes:
        props = note["properties"]

        # Basic GPX properties
        assert "name" in props
        assert props["name"].endswith(".jpg")
        assert "desc" in props
        assert props["desc"] == "Fotografía"
        assert "type" in props
        assert props["type"] == "photonote"
        assert "time" in props
        assert "link" in props
        assert props["link"].endswith(".jpg")

        # OsmAnd notes don't have individual extensions, just basic GPX properties

    # Check specific photo note with known data
    first_note = next(
        (
            f
            for f in result["features"]
            if f["properties"].get("name") == "YHpvuyI9--.1.jpg"
        ),
        None,
    )
    assert first_note is not None
    props = first_note["properties"]
    assert props["link"] == "YHpvuyI9--.1.jpg"
    assert props["time"] == "2025-05-03T21:53:38Z"

    # Check coordinates (should be in NYC area based on the data)
    _validate_coordinates_in_bounds(
        first_note["geometry"]["coordinates"], (-74.0, -73.0), (40.7, 40.8)
    )


def test_convert_data__osmand_poi_gpx(osmand_poi_gpx_file):
    """Test conversion of OsmAnd POI GPX data with comprehensive metadata."""
    result, output_format = convert_data(str(osmand_poi_gpx_file), "gpx")
    assert output_format == "geojson"

    # Root-level structure validation
    _validate_geojson_structure(result, 3)

    # All features should be waypoints (Points)
    points = [f for f in result["features"] if f["geometry"]["type"] == "Point"]
    assert len(points) == 3

    # Validate geometry and basic structure
    for feature in result["features"]:
        _validate_point_geometry(feature)

    # Validate specific POI features
    poi_features = [
        f for f in result["features"] if f["properties"].get("type") == "Guyana trip"
    ]
    assert len(poi_features) == 3

    # Check that all POI features have required properties
    for poi in poi_features:
        props = poi["properties"]

        # Basic GPX properties
        assert "name" in props
        assert "type" in props
        assert props["type"] == "Guyana trip"
        assert "time" in props

        # OsmAnd extensions should be captured (at least visited_date for all)
        assert "osmand:visited_date" in props or "visited_date" in props

    # Check specific POI with known data - A&D Sunset Guesthouse
    sunset_guesthouse = next(
        (
            f
            for f in result["features"]
            if f["properties"].get("name") == "A&D Sunset Guesthouse"
        ),
        None,
    )
    assert sunset_guesthouse is not None
    props = sunset_guesthouse["properties"]

    # Basic properties
    assert props["type"] == "Guyana trip"
    assert props["time"] == "2025-04-21T15:29:08Z"
    assert "ele" in props  # elevation
    assert float(props["ele"]) == 485.5

    # OsmAnd extensions
    _assert_osmand_property(props, "visited_date", "2025-04-26T16:57:24Z")

    # Check coordinates (should be in Guyana)
    _validate_coordinates_in_bounds(
        sunset_guesthouse["geometry"]["coordinates"], (-61.0, -60.0), (5.8, 5.9)
    )

    # Check Cara Lodge with comprehensive metadata
    cara_lodge = next(
        (f for f in result["features"] if f["properties"].get("name") == "Cara Lodge"),
        None,
    )
    assert cara_lodge is not None
    props = cara_lodge["properties"]

    # Basic properties
    assert props["type"] == "Guyana trip"
    assert props["time"] == "2025-04-27T23:57:35Z"

    # OsmAnd extensions
    _assert_osmand_property(props, "amenity_subtype", "hotel")
    _assert_osmand_property(props, "address", "Quamina Street, Alberttown")
    _assert_osmand_property(
        props, "amenity_origin", "Amenity:Cara Lodge: tourism:hotel"
    )
    _assert_osmand_property(props, "amenity_name", "Cara Lodge")
    _assert_osmand_property(props, "osm_tag_wikidata", "Q111880937")
    _assert_osmand_property(props, "icon", "tourism_hotel")
    _assert_osmand_property(props, "amenity_type", "tourism")
    _assert_osmand_property(props, "visited_date", "2025-05-20T16:14:43Z")

    # Check Hotel with minimal metadata
    hotel = next(
        (f for f in result["features"] if f["properties"].get("name") == "Hotel"), None
    )
    assert hotel is not None
    props = hotel["properties"]

    # Basic properties
    assert props["type"] == "Guyana trip"
    assert props["time"] == "2025-05-02T19:21:32Z"

    # OsmAnd extensions
    _assert_osmand_property(props, "address", "Quamina Street, Alberttown")
    _assert_osmand_property(props, "visited_date", "2025-05-02T19:22:14Z")


def test_osmand_data_consistency_across_formats(
    osmand_notes_gpx_file, osmand_poi_gpx_file
):
    """Test that OsmAnd GPX data is consistently parsed with all extensions captured."""
    notes_result, notes_format = convert_data(str(osmand_notes_gpx_file), "gpx")
    poi_result, poi_format = convert_data(str(osmand_poi_gpx_file), "gpx")

    assert notes_format == "geojson"
    assert poi_format == "geojson"

    # Notes should have 10 photo notes
    assert len(notes_result["features"]) == 10
    assert all(
        f["properties"].get("type") == "photonote" for f in notes_result["features"]
    )

    # POI should have 3 waypoints
    assert len(poi_result["features"]) == 3
    assert all(
        f["properties"].get("type") == "Guyana trip" for f in poi_result["features"]
    )

    # All features should have OsmAnd extensions captured
    for feature in poi_result["features"]:  # Only POI features have OsmAnd extensions
        props = feature["properties"]

        # Should have basic GPX properties
        assert "name" in props
        assert "type" in props
        assert "time" in props

        # Should have OsmAnd extensions (at least visited_date)
        _assert_osmand_property(props, "visited_date")

def test_slugify_empty_and_unicode():
    assert slugify(None) == "unnamed"
    assert slugify("") == "unnamed"
    assert slugify("Hello World!") == "hello-world"
    assert slugify("Café", allow_unicode=False) == "cafe"

