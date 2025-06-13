from pathlib import Path

import pandas as pd

from f.common_logic.file_operations import (
    detect_structured_data_type,
    save_data_to_file,
)


def test_save_data_to_file(tmp_path: Path):
    data = {"type": "FeatureCollection", "features": [{"type": "Feature"}]}
    save_data_to_file(data, "test", tmp_path, "geojson")
    file_path = tmp_path / "test.geojson"
    assert file_path.exists()


def test_save_data_to_file__no_data(tmp_path: Path):
    data = None
    save_data_to_file(data, "test", tmp_path, "geojson")
    assert not (tmp_path / "test.geojson").exists()


def test_save_data_to_file__no_features(tmp_path: Path):
    data = {"type": "FeatureCollection", "features": []}
    save_data_to_file(data, "test", tmp_path, "geojson")
    assert not (tmp_path / "test.geojson").exists()


def test_save_data_to_file__csv(tmp_path: Path):
    data = [["col1", "col2"], ["val1", "val2"]]
    save_data_to_file(data, "test", tmp_path, "csv")
    file_path = tmp_path / "test.csv"
    assert file_path.exists()
    content = file_path.read_text()
    assert '"col1","col2"' in content


def test_save_data_to_file__csv_empty(tmp_path: Path):
    data = []
    save_data_to_file(data, "test", tmp_path, "csv")
    file_path = tmp_path / "test.csv"
    assert not file_path.exists()


def test_structured_data_type__valid_gpx(tmp_path: Path):
    file_path = tmp_path / "test.gpx"
    file_path.write_text(
        "<?xml version='1.0' encoding='UTF-8'?><gpx version='1.1' creator='gpxpy' xmlns='http://www.topografix.com/GPX/1/1'><trk><trkseg><trkpt lat='48.8584' lon='2.3584'><ele>100</ele></trkpt></trkseg></trk></gpx>"
    )
    assert detect_structured_data_type(file_path) == "gpx"


def test_structured_data_type__valid_kml(tmp_path: Path):
    file_path = tmp_path / "test.kml"
    file_path.write_text(
        "<?xml version='1.0' encoding='UTF-8'?><kml xmlns='http://www.opengis.net/kml/2.2'><Document><Folder><Placemark><Point><coordinates>2.3584,48.8584</coordinates></Point></Placemark></Folder></Document></kml>"
    )
    assert detect_structured_data_type(file_path) == "kml"


def test_structured_data_type__valid_csv(tmp_path: Path):
    file_path = tmp_path / "test.csv"
    file_path.write_text("col1,col2\nval1,val2")
    assert detect_structured_data_type(file_path) == "csv"


def test_structured_data_type__invalid_csv(tmp_path: Path):
    file_path = tmp_path / "test.csv"
    file_path.write_text("key:value,value,more\n1:2:3\nnot,really,a,csv")
    assert not detect_structured_data_type(file_path) == "csv"


def test_structured_data_type__valid_json(tmp_path: Path):
    file_path = tmp_path / "test.json"
    file_path.write_text(
        """
        {
            "key": "value"
        }
        """
    )
    assert detect_structured_data_type(file_path) == "json"


def test_structured_data_type__malformed_json(tmp_path: Path):
    file_path = tmp_path / "bad.json"
    file_path.write_text("{ this is not valid json }")
    assert detect_structured_data_type(file_path) == "unsupported"


def test_structured_data_type__valid_geojson(tmp_path: Path):
    file_path = tmp_path / "test.geojson"
    file_path.write_text(
        '{"type": "FeatureCollection", "features": [{"type": "Feature"}]}'
    )
    assert detect_structured_data_type(file_path) == "geojson"


def test_structured_data_type__valid_excel(tmp_path: Path):
    file_path = tmp_path / "test.xlsx"
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    df.to_excel(file_path, index=False)

    assert detect_structured_data_type(file_path) == "xlsx"

    file_path = tmp_path / "test.xls"
    df.to_excel(file_path, index=False)

    assert detect_structured_data_type(file_path) == "xls"


def test_structured_data_type__pdf_file(tmp_path: Path):
    file_path = tmp_path / "table.html"
    file_path.write_text(
        "<html><body><table><tr><td>Not</td></tr></table></body></html>"
    )
    assert detect_structured_data_type(file_path) == "unsupported"


def test_structured_data_type__empty_file(tmp_path: Path):
    file_path = tmp_path / "empty.geojson"
    file_path.write_text("")
    assert detect_structured_data_type(file_path) == "unsupported"
