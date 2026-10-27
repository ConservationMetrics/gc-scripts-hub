"""Exercise file inputs and downloads through the actual Windmill entry point."""

import base64
import copy
import inspect
import json
from pathlib import Path

import pytest
import yaml
from defusedxml.common import DefusedXmlException

from f.export.umap.odk_to_umap import main

ROOT = Path(__file__).resolve().parents[1]
FIELDS = [
    f"/data/survey_questions/grp_bldg/{name}"
    for name in ("building", "building_levels")
]


def inputs():
    return {
        "geojson": (ROOT / "example/synthetic.geojson").read_bytes(),
        "columns": (ROOT / "example/columns.json").read_bytes(),
        "form_xml": (ROOT / "example/form.xml").read_bytes(),
        "language": "english(en)",
        "fields": FIELDS,
        "geometry": "feature-point",
    }


def decode(result):
    assert set(result) == {"render_all"}
    files = {}
    for item in result["render_all"]:
        assert set(item) == {"file"}
        file = item["file"]
        files[file["filename"]] = json.loads(
            base64.b64decode(file["content"], validate=True)
        )
    assert set(files) == {"survey.umap", "survey.report.json"}
    return files["survey.umap"], files["survey.report.json"]


@pytest.mark.parametrize("geometry", ["feature-point", "outline"])
def test_downloads_preserve_answers_wording_geometry_colors_and_selected_ids(geometry):
    args = inputs()
    args.update(
        geometry=geometry,
        identifiers=["entity_id", "submission_id", "submission_version_id"],
    )
    before = copy.deepcopy(args)
    document, report = decode(main(**args))
    assert args == before
    assert report["feature_count"] == 2
    features = document["layers"][0]["features"]
    assert [f["id"] for f in features] == ["survey-1", "survey-2"]
    assert [f["properties"][FIELDS[1]] for f in features] == [2, 4]
    assert [f["properties"]["_odk_display_0"] for f in features] == [
        "Industrial",
        "Industrial",
    ]
    assert [f["properties"]["_umap_options"]["color"] for f in features] == [
        "#00ffff",
        "#800080",
    ]
    assert (
        "Number of stories"
        in document["layers"][0]["properties"]["popupContentTemplate"]
    )
    for number, feature in enumerate(features, 1):
        expected = {"type": "Point", "coordinates": [number, number]}
        if geometry == "outline":
            expected = {
                "type": "Polygon",
                "coordinates": [
                    [
                        [number, number],
                        [number + 0.01, number],
                        [number, number + 0.01],
                        [number, number],
                    ]
                ],
            }
        assert feature["geometry"] == expected
        assert (
            feature["properties"]["_odk_submission_version_id"] == f"version-{number}"
        )
        assert (
            feature["properties"]["_odk_entity_id"]
            == f"{number:08d}-1111-4111-8111-111111111111"
        )


def test_unselected_properties_and_ids_do_not_reach_either_download():
    document, report = decode(main(**inputs()))
    text = json.dumps([document, report])
    for marker in (
        "CANARY",
        "survey-1",
        "version-1",
        "1111-4111",
        "__system",
        "building_material",
    ):
        assert marker not in text


def test_survey_point_uses_collected_location_instead_of_feature_location():
    args = inputs()
    data = json.loads(args["geojson"])
    for feature in data["features"]:
        props = feature["properties"]
        group = json.loads(props["survey_questions"])
        group["grp_loc"]["geopoint"] = {
            "Latitude": 5,
            "Longitude": 6,
            "Altitude": 10,
            "Accuracy": 2,
        }
        props["survey_questions"] = json.dumps(group)
    args.update(geojson=json.dumps(data).encode(), geometry="survey-point")
    document, _ = decode(main(**args))
    assert all(
        f["geometry"] == {"type": "Point", "coordinates": [6, 5]}
        for f in document["layers"][0]["features"]
    )


def test_missing_answer_stays_null_and_displays_dash():
    args = inputs()
    data = json.loads(args["geojson"])
    props = data["features"][0]["properties"]
    group = json.loads(props["survey_questions"])
    group["grp_bldg"]["building_levels"] = None
    props["survey_questions"] = json.dumps(group)
    args["geojson"] = json.dumps(data).encode()
    document, report = decode(main(**args))
    assert document["layers"][0]["features"][0]["properties"][FIELDS[1]] is None
    assert report["records"][0]["display"][1]["display"] == "—"


def test_conflicting_csv_form_version_is_rejected():
    args = inputs()
    data, mapping = json.loads(args["geojson"]), json.loads(args["columns"])
    mapping["FormVersion"] = "FormVersion"
    for feature in data["features"]:
        feature["properties"]["FormVersion"] = "v1"
    args.update(geojson=json.dumps(data).encode(), columns=json.dumps(mapping).encode())
    decode(main(**args))
    data["features"][1]["properties"]["FormVersion"] = "v2"
    args["geojson"] = json.dumps(data).encode()
    with pytest.raises(ValueError, match="Record 2:.*version"):
        main(**args)


@pytest.mark.parametrize(
    "problem", ["form_version", "stable_id", "outline", "color", "answer", "duplicate"]
)
def test_invalid_second_record_returns_no_downloads(problem):
    args = inputs()
    data = json.loads(args["geojson"])
    props = data["features"][1]["properties"]
    if problem == "form_version":
        props["odk_form_version"] = "another-version"
    elif problem == "stable_id":
        props["odk_submission_id"] = "another-survey"
    elif problem == "outline":
        args["geometry"] = "outline"
        group = json.loads(props["survey_questions"])
        group["grp_loc"]["building_outline"] = None
        props["survey_questions"] = json.dumps(group)
    elif problem == "color":
        props["marker_color"] = "javascript:alert(1)"
    elif problem == "answer":
        group = json.loads(props["survey_questions"])
        group["grp_bldg"]["building_levels"] = "<script>alert(1)</script>"
        props["survey_questions"] = json.dumps(group)
    else:
        data["features"][1] = data["features"][0]
    args["geojson"] = json.dumps(data).encode()
    with pytest.raises(ValueError, match="Record 2:"):
        main(**args)


@pytest.mark.parametrize(
    "key,value",
    [
        ("fields", []),
        ("fields", "all"),
        ("identifiers", "all"),
        ("columns", b"[]"),
        ("columns", b'{"meta": 7}'),
        ("geojson", b"bad json"),
        ("geojson", b"[]"),
        ("form_xml", b""),
        ("form_xml", b"x" * (5 * 1024 * 1024 + 1)),
    ],
)
def test_invalid_uploads_and_selections_fail(key, value):
    args = inputs()
    args[key] = value
    with pytest.raises(ValueError):
        main(**args)


def test_xml_entities_are_rejected():
    args = inputs()
    args["form_xml"] = (
        b'<!DOCTYPE x [<!ENTITY secret SYSTEM "file:///etc/passwd">]><x>&secret;</x>'
    )
    with pytest.raises(DefusedXmlException):
        main(**args)


@pytest.mark.parametrize("problem", ["empty_choices", "missing_dataset"])
def test_incomplete_form_metadata_is_rejected(problem):
    args = inputs()
    if problem == "empty_choices":
        args["form_xml"] = args["form_xml"].replace(
            b"<item><name>industrial</name><itextId>industrial-label</itextId></item>",
            b"",
        )
        message = "Empty internal choice list"
    else:
        args["form_xml"] = args["form_xml"].replace(b' dataset="buildings"', b"")
        message = "entity dataset"
    with pytest.raises(ValueError, match=message):
        main(**args)


def test_schema_matches_callable_and_marks_file_inputs():
    metadata = yaml.safe_load((ROOT / "odk_to_umap.script.yaml").read_text())
    schema = metadata["schema"]
    signature = inspect.signature(main)
    assert set(schema["properties"]) == set(signature.parameters)
    assert set(schema["required"]) == {
        name
        for name, p in signature.parameters.items()
        if p.default is inspect.Parameter.empty
    }
    for name in ("geojson", "columns", "form_xml"):
        assert schema["properties"][name]["format"] == "base64"
        assert signature.parameters[name].annotation is bytes
