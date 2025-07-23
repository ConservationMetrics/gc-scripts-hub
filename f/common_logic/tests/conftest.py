import json
from pathlib import Path

import pytest


@pytest.fixture
def locusmap_points_gpx_file():
    return Path(__file__).parent / "assets" / "locusmap_favorites.gpx"


@pytest.fixture
def locusmap_points_kml_file():
    return Path(__file__).parent / "assets" / "locusmap_favorites.kml"


@pytest.fixture
def locusmap_tracks_gpx_file():
    return Path(__file__).parent / "assets" / "locusmap_tracks.gpx"


@pytest.fixture
def locusmap_tracks_kml_file():
    return Path(__file__).parent / "assets" / "locusmap_tracks.kml"


@pytest.fixture
def garmin_sample_gpx_file():
    return Path(__file__).parent / "assets" / "garmin_sample.gpx"


@pytest.fixture
def kobotoolbox_csv_file():
    return Path(__file__).parent / "assets" / "kobotoolbox_submissions.csv"


@pytest.fixture
def kobotoolbox_excel_file():
    return Path(__file__).parent / "assets" / "kobotoolbox_submissions.xlsx"


@pytest.fixture
def kobotoolbox_multiple_sheets_excel_file():
    return (
        Path(__file__).parent
        / "assets"
        / "kobotoolbox_submissions_multiple_sheets.xlsx"
    )


@pytest.fixture
def kobotoolbox_empty_submission_csv_file(tmp_path):
    path = tmp_path / "empty_kobo.csv"
    path.write_text("start,location,comment\n")
    return path


@pytest.fixture
def mapeo_geojson_file():
    return Path(__file__).parent / "assets" / "mapeo_observations.geojson"


@pytest.fixture
def empty_geojson_file(tmp_path):
    path = tmp_path / "empty.geojson"
    path.write_text(json.dumps({"type": "FeatureCollection", "features": []}))
    return path


@pytest.fixture
def geojson_with_missing_properties_file(tmp_path):
    path = tmp_path / "missing_props.geojson"
    data = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}},
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [1, 1]},
                "properties": None,
            },
        ],
    }
    path.write_text(json.dumps(data))
    return path


@pytest.fixture
def geojson_with_invalid_geometry_file(tmp_path):
    path = tmp_path / "invalid_geometry.geojson"
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": "foo,bar"},
                "properties": {},
            },
            {"type": "Feature", "geometry": None, "properties": {"name": "ghost"}},
        ],
    }
    path.write_text(json.dumps(data))
    return path


@pytest.fixture
def geojson_with_invalid_top_level_structure_file(tmp_path):
    path = tmp_path / "invalid_top_level.geojson"
    data = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": "foo,bar"},
        "properties": {},
    }
    path.write_text(json.dumps(data))
    return path


@pytest.fixture
def googleearth_sample_kml_file():
    return Path(__file__).parent / "assets" / "googleearth_sample.kml"


@pytest.fixture
def alerts_kml_file():
    return Path(__file__).parent / "assets" / "gc_alerts.kml"


@pytest.fixture
def kml_with_missing_geometry_file(tmp_path):
    path = tmp_path / "kml_missing_geometry.kml"
    data = """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <Placemark>
      <n>Missing Geometry</n>
      <description>No geometry provided</description>
    </Placemark>
  </Document>
</kml>"""
    path.write_text(data)
    return path


@pytest.fixture
def osm_overpass_gpx_file():
    return Path(__file__).parent / "assets" / "osm_overpass.gpx"


@pytest.fixture
def osm_overpass_geojson_file():
    return Path(__file__).parent / "assets" / "osm_overpass.geojson"


@pytest.fixture
def osm_overpass_kml_file():
    return Path(__file__).parent / "assets" / "osm_overpass.kml"
