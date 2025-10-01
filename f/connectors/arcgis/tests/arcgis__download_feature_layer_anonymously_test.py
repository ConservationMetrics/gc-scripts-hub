# ----------------------
# Unit tests (pytest)
# ----------------------

if __name__ == "__main__":
    # Quick smoke when run directly
    logging.basicConfig(level=logging.INFO)
    print("Module loaded. Run pytest to execute unit tests.")


# Pytest tests below. Save as separate file in practice; kept here for convenience.


def _create_test_session(monkeypatch, metadata_resp: Dict[str, Any], query_resp: Dict[str, Any]):
    """Helper to create a fake session by monkeypatching requests.Session.get"""
    class DummyResp:
        def __init__(self, data, status=200):
            self._data = data
            self.status_code = status

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"{self.status_code} error")

        def json(self):
            return self._data

        def iter_content(self, chunk_size=8192):
            yield b"abc"

    class DummySession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, **kwargs):
            self.calls.append((url, params))
            if url.endswith("?f=pjson"):
                return DummyResp(metadata_resp)
            if "/query" in url:
                return DummyResp(query_resp)
            # attachments info or download
            if url.endswith("/attachments"):
                return DummyResp({"attachmentInfos": []})
            if "/attachments/" in url:
                return DummyResp(b"binarycontent")
            return DummyResp({}, status=404)

    return DummySession()


# The tests below use pytest style. They assume pytest is available where run.

def test_slugify_empty_and_unicode():
    assert slugify(None) == "unnamed"
    assert slugify("") == "unnamed"
    assert slugify("Hello World!") == "hello-world"
    assert slugify("Café", allow_unicode=False) == "cafe"


def test_get_layer_metadata_and_fetch_features(monkeypatch, tmp_path):
    metadata = {"layers": [{"id": 2, "name": "Test Layer"}]}
    features = {"features": [{"attributes": {"OBJECTID": 1}, "geometry": {"x": 1113194.907, "y": 1118889.974}}]}

    session = _create_test_session(monkeypatch, metadata, features)

    # call functions
    md = get_layer_metadata(session, "sub", "svc", "feat")
    assert "layers" in md

    records = fetch_features(session, "https://sub.arcgis.com/svc/FeatureServer", layer_index=2)
    assert isinstance(records, list) and records
    assert records[0]["OBJECTID"] == 1


def test_transform_and_build_geojson(monkeypatch):
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    rec = {"OBJECTID": 1, "__geometry": {"x": 1113194.907, "y": 1118889.974}}
    transform_record_geometry(rec, transformer)
    assert "__geojson_geometry" in rec
    gc = build_geojson([rec])
    assert gc["type"] == "FeatureCollection"
    assert gc["features"][0]["geometry"]["type"] == "Point"


def test_fetch_data_flow(monkeypatch, tmp_path):
    metadata = {"layers": [{"id": 2, "name": "Test Layer"}]}
    features = {"features": [{"attributes": {"OBJECTID": 1, "name": "A"}, "geometry": {"x": 1113194.907, "y": 1118889.974}}]}
    session = _create_test_session(monkeypatch, metadata, features)

    # monkeypatch save_data_to_file to avoid external dependency
    saved = {}

    def fake_save(obj, name, storage_path, file_type=None):
        saved["name"] = name
        saved["path"] = storage_path
        saved["obj"] = obj

    monkeypatch.setattr("f.common_logic.file_operations.save_data_to_file", fake_save)

    out = fetch_data(
        subdomain="sub",
        service_id="svc",
        feature_id="feat",
        layer_index=2,
        download_attachments=False,
        output_format="geojson",
        storage_path=tmp_path,
        session=session,
    )

    assert out.exists()
    assert "name" in saved and saved["name"].endswith(".geojson")
