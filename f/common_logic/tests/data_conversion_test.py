import pytest

from f.common_logic.data_conversion import convert_data


def test_convert_data__locusmap_points_gpx(locusmap_points_gpx_file):
    result = convert_data(str(locusmap_points_gpx_file), "gpx")
    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) >= 2  # At least 2 waypoints

    # Filter to waypoints with descriptions (the main features we care about)
    waypoints_with_desc = [
        f
        for f in result["features"]
        if f["geometry"]["type"] == "Point" and "desc" in f["properties"]
    ]
    assert len(waypoints_with_desc) >= 2

    for feature in waypoints_with_desc:
        geometry = feature["geometry"]
        properties = feature["properties"]

        assert geometry["type"] == "Point"
        assert "coordinates" in geometry
        assert isinstance(properties, dict)

        assert "desc" in properties
        assert any(word in properties["desc"].lower() for word in ["tree", "rock"])

        assert "link" in properties
        assert any(properties["link"].strip().endswith(ext) for ext in [".jpg", ".m4a"])


def test_convert_data__locusmap_points_kml(locusmap_points_kml_file):
    result = convert_data(str(locusmap_points_kml_file), "kml")
    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 2

    for feat in result["features"]:
        assert feat["geometry"]["type"] == "Point"
        assert "coordinates" in feat["geometry"]
        props = feat["properties"]
        assert isinstance(props, dict)
        assert "name" in props
        assert "description" in props
        assert "attachments" in props


def test_convert_data__locusmap_tracks_kml(locusmap_tracks_kml_file):
    result = convert_data(str(locusmap_tracks_kml_file), "kml")

    # Root-level structure check
    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 4

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
    props = pt["properties"]

    assert "attachments" in props
    assert props["attachments"].endswith(".jpg")
    assert "description" in props
    assert "willow" in props["description"].lower()


def test_convert_data__locusmap_tracks_gpx(locusmap_tracks_gpx_file):
    result = convert_data(str(locusmap_tracks_gpx_file), "gpx")

    # Root-level sanity checks
    assert result["type"] == "FeatureCollection"
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
    result = convert_data(str(garmin_sample_gpx_file), "gpx")
    assert result["type"] == "FeatureCollection"
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
    result = convert_data(str(kobotoolbox_csv_file), "csv")

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
    result = convert_data(str(kobotoolbox_excel_file), "xlsx")
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
    result = convert_data(str(file), "json")
    assert result == [["a", "b"], ["1", "2"], ["3", ""]]


def test_convert_data__json_empty(tmp_path):
    file = tmp_path / "test_empty.json"
    file.write_text("[]")
    with pytest.raises(ValueError, match="JSON file contains no records"):
        convert_data(str(file), "json")


def test_read_data__mapeo_geojson(mapeo_geojson_file):
    result = convert_data(str(mapeo_geojson_file), "geojson")
    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 3

    for feature in result["features"]:
        assert feature["type"] == "Feature"
        assert "geometry" in feature
        assert "properties" in feature
        assert isinstance(feature["properties"], dict)
        assert "type" in feature["properties"]
        assert "notes" in feature["properties"]


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
    result = convert_data(str(googleearth_sample_kml_file), "kml")
    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 3

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
    result = convert_data(str(alerts_kml_file), "kml")
    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 2

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
    import pytest

    with pytest.raises(ValueError):
        convert_data("/fake/path.foo", "foo")
