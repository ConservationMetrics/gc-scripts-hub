import base64
import zipfile
from pathlib import Path

import pytest

from f.common_logic.file_operations import save_data_to_file, save_uploaded_file_to_temp


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


def test_save_uploaded_file_to_temp__single_file(tmp_path: Path):
    input_path = tmp_path / "sample.csv"
    input_path.write_text("col1,col2\nval1,val2")

    with input_path.open("rb") as f:
        encoded = base64.b64encode(f.read()).decode()

    result = save_uploaded_file_to_temp(
        [{"name": "sample.csv", "data": encoded}], tmp_dir=str(tmp_path)
    )

    assert "file_paths" in result
    saved_path = Path(result["file_paths"][0])
    assert saved_path.exists()
    assert saved_path.read_text() == "col1,col2\nval1,val2"


def test_save_uploaded_file_to_temp__zip_multiple_files(tmp_path: Path):
    file1 = tmp_path / "one.csv"
    file1.write_text("a,b\n1,2")
    file2 = tmp_path / "two.csv"
    file2.write_text("c,d\n3,4")

    zip_path = tmp_path / "multiple.zip"
    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.write(file1, arcname="one.csv")
        zipf.write(file2, arcname="two.csv")

    encoded = base64.b64encode(zip_path.read_bytes()).decode()

    result = save_uploaded_file_to_temp(
        [{"name": "multiple.zip", "data": encoded}], tmp_dir=str(tmp_path)
    )

    assert "file_paths" in result
    paths = [Path(p) for p in result["file_paths"]]
    assert any("one.csv" in str(p) for p in paths)
    assert any("two.csv" in str(p) for p in paths)
    for p in paths:
        assert p.exists()
    assert not (tmp_path / "multiple.zip").exists()


def test_save_uploaded_file_to_temp__bad_input(tmp_path: Path):
    result = save_uploaded_file_to_temp(
        [{"name": "corrupt.txt", "data": "!!!not base64!!!"}], tmp_dir=str(tmp_path)
    )
    assert "error" in result


@pytest.fixture
def zip_with_subdir(tmp_path: Path):
    zip_path = tmp_path / "bundle.zip"
    data_dir = tmp_path / "attachments"
    data_dir.mkdir()

    (tmp_path / "observations.geojson").write_text(
        '{"type": "FeatureCollection", "features": []}'
    )
    (data_dir / "photo1.jpg").write_text("fake jpg data 1")
    (data_dir / "photo2.jpg").write_text("fake jpg data 2")

    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.write(tmp_path / "observations.geojson", arcname="observations.geojson")
        zipf.write(data_dir / "photo1.jpg", arcname="attachments/photo1.jpg")
        zipf.write(data_dir / "photo2.jpg", arcname="attachments/photo2.jpg")

    return zip_path


def test_save_uploaded_file_to_temp__zip_with_subdir(
    zip_with_subdir: Path, tmp_path: Path
):
    encoded = base64.b64encode(zip_with_subdir.read_bytes()).decode()

    result = save_uploaded_file_to_temp(
        [{"name": "bundle.zip", "data": encoded}], tmp_dir=str(tmp_path)
    )

    assert "file_paths" in result
    paths = [Path(p) for p in result["file_paths"]]
    assert any("observations.geojson" in str(p) for p in paths)
    assert any("attachments/photo1.jpg" in str(p) for p in paths)
    assert any("attachments/photo2.jpg" in str(p) for p in paths)
    for p in paths:
        assert p.exists()
    assert not zip_with_subdir.exists()


def test_save_uploaded_file_to_temp__kmz_with_subdir(
    zip_with_subdir: Path, tmp_path: Path
):
    kmz_path = tmp_path / "bundle.kmz"
    kmz_path.write_bytes(zip_with_subdir.read_bytes())  # clone the zip as .kmz

    encoded = base64.b64encode(kmz_path.read_bytes()).decode()

    result = save_uploaded_file_to_temp(
        [{"name": "bundle.kmz", "data": encoded}], tmp_dir=str(tmp_path)
    )

    assert "file_paths" in result
    paths = [Path(p) for p in result["file_paths"]]
    # OK, a KMZ should contain a KML file and not GeoJSON, but we're not
    # testing that here
    assert any("observations.geojson" in str(p) for p in paths)
    assert any("attachments/photo1.jpg" in str(p) for p in paths)
    assert any("attachments/photo2.jpg" in str(p) for p in paths)
    for p in paths:
        assert p.exists()
    assert not kmz_path.exists()
