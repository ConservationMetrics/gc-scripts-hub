from pathlib import Path

import pandas as pd

from f.common_logic.data_conversion import detect_structured_data_type


def test_structured_data_type__gpx_format(tmp_path: Path):
    file_path = tmp_path / "test.gpx"
    file_path.write_text(
        "<?xml version='1.0' encoding='UTF-8'?><gpx version='1.1' creator='gpxpy' xmlns='http://www.topografix.com/GPX/1/1'><trk><trkseg><trkpt lat='48.8584' lon='2.3584'><ele>100</ele></trkpt></trkseg></trk></gpx>"
    )
    assert detect_structured_data_type(file_path) == "gpx"


def test_structured_data_type__kml_format(tmp_path: Path):
    file_path = tmp_path / "test.kml"
    file_path.write_text(
        "<?xml version='1.0' encoding='UTF-8'?><kml xmlns='http://www.opengis.net/kml/2.2'><Document><Folder><Placemark><Point><coordinates>2.3584,48.8584</coordinates></Point></Placemark></Folder></Document></kml>"
    )
    assert detect_structured_data_type(file_path) == "kml"


def test_structured_data_type__csv_format(tmp_path: Path):
    file_path = tmp_path / "test.csv"
    file_path.write_text("col1,col2\nval1,val2")
    assert detect_structured_data_type(file_path) == "csv"


def test_structured_data_type__json_format(tmp_path: Path):
    file_path = tmp_path / "test.json"
    file_path.write_text(
        """
        {
            "key": "value"
        }
        """
    )
    assert detect_structured_data_type(file_path) == "json"


def test_structured_data_type__geojson_format(tmp_path: Path):
    file_path = tmp_path / "test.geojson"
    file_path.write_text(
        '{"type": "FeatureCollection", "features": [{"type": "Feature"}]}'
    )
    assert detect_structured_data_type(file_path) == "geojson"


def test_structured_data_type__geojson_with_json_extension(tmp_path: Path):
    """Test that GeoJSON files with .json extension are correctly detected."""
    file_path = tmp_path / "test.json"
    file_path.write_text(
        '{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {}}]}'
    )
    assert detect_structured_data_type(file_path) == "geojson"


def test_structured_data_type__geojson_with_json_extension_minimal(tmp_path: Path):
    """Test minimal valid GeoJSON with .json extension."""
    file_path = tmp_path / "minimal.json"
    file_path.write_text('{"type": "FeatureCollection", "features": []}')
    # This should still be detected as json because read_geojson would reject empty features
    # but the detection should identify it as geojson based on structure
    assert detect_structured_data_type(file_path) == "geojson"


def test_structured_data_type__regular_json_with_json_extension(tmp_path: Path):
    """Test that regular JSON files are still detected as JSON, not GeoJSON."""
    file_path = tmp_path / "regular.json"
    file_path.write_text('{"name": "John", "age": 30, "city": "New York"}')
    assert detect_structured_data_type(file_path) == "json"


def test_structured_data_type__excel_format(tmp_path: Path):
    file_path = tmp_path / "test.xlsx"
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    df.to_excel(file_path, index=False)

    assert detect_structured_data_type(file_path) == "xlsx"

    # Note: pandas creates newer Excel format even for .xls extension
    # MIME detection correctly identifies the actual format
    file_path = tmp_path / "test.xls"
    df.to_excel(file_path, index=False)

    assert detect_structured_data_type(file_path) == "xlsx"


def test_structured_data_type__kobotoolbox_excel_export():
    file_path = Path(__file__).parent / "assets" / "kobotoolbox_submissions.xlsx"
    assert detect_structured_data_type(file_path) == "xlsx"


def test_structured_data_type__pdf_file(tmp_path: Path):
    file_path = tmp_path / "table.html"
    file_path.write_text(
        "<html><body><table><tr><td>Not</td></tr></table></body></html>"
    )
    assert detect_structured_data_type(file_path) == "unsupported"


def test_structured_data_type__smart_xml_format(tmp_path: Path):
    """Test that SMART patrol XML files are correctly detected."""
    file_path = tmp_path / "patrol.xml"
    file_path.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<patrol xmlns:ns2="http://www.smartconservationsoftware.org/xml/1.3/patrol" id="p1">'
        '<ns2:objective><ns2:description>Test</ns2:description></ns2:objective>'
        '</patrol>'
    )
    assert detect_structured_data_type(file_path) == "smart"


def test_structured_data_type__smart_xml_with_root_namespace(tmp_path: Path):
    """Test SMART XML detection when namespace is on root element."""
    file_path = tmp_path / "patrol2.xml"
    file_path.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ns2:patrol xmlns:ns2="http://www.smartconservationsoftware.org/xml/1.3/patrol" id="p1">'
        '<ns2:legs id="l1"></ns2:legs>'
        '</ns2:patrol>'
    )
    assert detect_structured_data_type(file_path) == "smart"


def test_structured_data_type__generic_xml_format(tmp_path: Path):
    """Test that generic XML files without SMART namespace are detected as generic xml."""
    file_path = tmp_path / "generic.xml"
    file_path.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<root><child>value</child></root>'
    )
    assert detect_structured_data_type(file_path) == "xml"
