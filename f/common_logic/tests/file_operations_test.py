from pathlib import Path

from f.common_logic.save_disk import (
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
