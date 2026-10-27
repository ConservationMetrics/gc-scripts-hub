"""Synthetic batch and field-selection tests; no HOT records or credentials."""

import copy
import json
import tempfile
import unittest
from pathlib import Path

from f.common_logic.odk_umap import export_map, read_form

QUESTIONS = [
    "/data/survey_questions/grp_bldg/" + name
    for name in ("building", "building_levels", "building_material")
]
FORM_XML = """<html xmlns="http://www.w3.org/2002/xforms"><model>
<instance><data id="synthetic-form" version="v1">
<meta><entity dataset="buildings"/></meta></data></instance>
<instance id="uses"><root><item><name>industrial</name>
<itextId>industrial-label</itextId></item></root></instance>
<itext><translation lang="english(en)">
<text id="/data/survey_questions/grp_bldg/building:label">
<value>Building use</value></text>
<text id="/data/survey_questions/grp_bldg/building_levels:label">
<value>Number of stories</value></text>
<text id="/data/survey_questions/grp_bldg/building_material:label">
<value>Wall material</value></text>
<text id="industrial-label"><value>Industrial</value></text>
</translation></itext></model><body>
<select1 ref="/data/survey_questions/grp_bldg/building">
<itemset nodeset="instance('uses')/root/item"/></select1>
<input ref="/data/survey_questions/grp_bldg/building_levels"/>
<input ref="/data/survey_questions/grp_bldg/building_material"/>
</body></html>"""


def fixture():
    features = []
    for number, shade in [(1, "#00ffff"), (2, "#800080")]:
        entity = f"{number:08d}-1111-4111-8111-111111111111"
        props = {
            "dataset_name": "synthetic-form",
            "odk_form_version": "v1",
            "odk_submission_id": f"survey-{number}",
            "odk_submission_version_id": f"version-{number}",
            "meta": json.dumps(
                {"instanceID": f"version-{number}", "secret": "META_CANARY"}
            ),
            "feature": entity,
            "odk_declared_entity_id": entity,
            "odk_entity_dataset": "buildings",
            "odk_entity_create": "1",
            "odk_entity_update": None,
            "xlocation": f"{number} {number} 0 0",
            "fill": shade,
            "stroke": shade,
            "marker_color": shade,
            "stroke_width": 3,
            "survey_questions": json.dumps(
                {
                    "grp_bldg": {
                        "building": "industrial",
                        "building_levels": number * 2,
                        "building_material": "UNSELECTED_ANSWER_CANARY",
                    },
                    "grp_loc": {
                        "building_outline": f"{number} {number} 0 0;"
                        f"{number} {number + 0.01} 0 0;"
                        f"{number + 0.01} {number} 0 0;{number} {number} 0 0"
                    },
                    "private_group": {"email": "NESTED_SECRET_CANARY"},
                }
            ),
            "__system": {"submitter": {"email": "SYSTEM_CANARY"}},
            "extra": "FLAT_SECRET_CANARY",
        }
        features.append(
            {
                "type": "Feature",
                "id": f"survey-{number}",
                "geometry": None,
                "properties": props,
                "foreign": "FOREIGN_CANARY",
            }
        )
    return {
        "type": "FeatureCollection",
        "features": features,
        "foreign": "ROOT_CANARY",
    }, {key: key for key in features[0]["properties"]}


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.data, self.mapping = fixture()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.form_path = Path(self.directory.name) / "form.xml"
        self.form_path.write_text(FORM_XML)
        self.form = read_form(self.form_path, "english(en)", QUESTIONS[:2])

    def run_export(self, **kwargs):
        return export_map(
            self.data,
            self.mapping,
            self.form,
            "feature-point",
            fields=QUESTIONS[:2],
            **kwargs,
        )

    def test_batch_keeps_distinct_answers_colors_and_optional_ids(self):
        original = copy.deepcopy(self.data)
        document, report = self.run_export(
            identifiers=["entity_id", "submission_id", "submission_version_id"]
        )
        features = document["layers"][0]["features"]
        self.assertEqual(len(features), 2)
        self.assertEqual([f["id"] for f in features], ["survey-1", "survey-2"])
        self.assertEqual([f["properties"][QUESTIONS[1]] for f in features], [2, 4])
        self.assertEqual(
            [f["properties"]["_umap_options"]["color"] for f in features],
            ["#00ffff", "#800080"],
        )
        self.assertEqual(
            [r["display"][0]["display"] for r in report["records"]],
            ["Industrial", "Industrial"],
        )
        self.assertEqual(self.data, original)
        self.assertEqual(document["geometry"]["coordinates"][0], 1.5)
        self.assertLess(document["properties"]["zoom"], 22)

    def test_whole_map_and_report_exclude_unselected_data(self):
        document, report = self.run_export()
        serialized = json.dumps([document, report])
        for marker in [
            "CANARY",
            "survey-1",
            "survey-2",
            "version-1",
            "version-2",
            "1111-4111",
            "__system",
            "private_group",
            "entity_dataset",
        ]:
            self.assertNotIn(marker, serialized)
        expected = set(QUESTIONS[:2]) | {
            "_odk_display_0",
            "_odk_display_1",
            "_umap_options",
        }
        for feature in document["layers"][0]["features"]:
            self.assertEqual(set(feature), {"type", "geometry", "properties"})
            self.assertEqual(set(feature["properties"]), expected)

    def test_only_chosen_question_appears_in_popup_and_report(self):
        document, report = export_map(
            self.data, self.mapping, self.form, "feature-point", fields=[QUESTIONS[1]]
        )
        popup = document["layers"][0]["properties"]["popupContentTemplate"]
        self.assertIn("Number of stories", popup)
        self.assertNotIn("Building use", popup)
        self.assertEqual(len(report["records"][0]["display"]), 1)
        self.assertNotIn("industrial", json.dumps(document))

    def test_outline_batch_preserves_both_rings(self):
        document, _ = export_map(
            self.data, self.mapping, self.form, "outline", fields=QUESTIONS[:2]
        )
        for number, feature in enumerate(document["layers"][0]["features"], 1):
            self.assertEqual(
                feature["geometry"],
                {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [number, number],
                            [number + 0.01, number],
                            [number, number + 0.01],
                            [number, number],
                        ]
                    ],
                },
            )

    def test_api_polygon_outline_matches_string_outline(self):
        expected, _ = export_map(
            self.data, self.mapping, self.form, "outline", fields=QUESTIONS[:2]
        )
        group = json.loads(self.data["features"][1]["properties"]["survey_questions"])
        group["grp_loc"]["building_outline"] = expected["layers"][0]["features"][1][
            "geometry"
        ]
        self.data["features"][1]["properties"]["survey_questions"] = json.dumps(group)
        actual, _ = export_map(
            self.data, self.mapping, self.form, "outline", fields=QUESTIONS[:2]
        )
        self.assertEqual(actual, expected)

    def test_missing_translated_choice_label_has_clear_error(self):
        self.form_path.write_text(
            FORM_XML.replace('id="industrial-label"', 'id="different-label"')
        )
        with self.assertRaisesRegex(
            ValueError, "Missing choice code or translated label.*building"
        ):
            read_form(self.form_path, "english(en)", QUESTIONS[:2])

    def test_second_visit_not_merged_by_building_id(self):
        self.data["features"][1]["properties"]["feature"] = self.data["features"][0][
            "properties"
        ]["feature"]
        self.data["features"][1]["properties"]["odk_declared_entity_id"] = self.data[
            "features"
        ][0]["properties"]["feature"]
        document, _ = self.run_export(identifiers=["entity_id"])
        self.assertEqual(len(document["layers"][0]["features"]), 2)

    def test_bad_second_record_rejects_whole_batch(self):
        self.data["features"][1]["properties"]["odk_form_version"] = "v2"
        with self.assertRaisesRegex(ValueError, "Record 2:.*version"):
            self.run_export()

    def test_duplicate_survey_id_rejected(self):
        self.data["features"][1] = copy.deepcopy(self.data["features"][0])
        with self.assertRaisesRegex(ValueError, "Record 2: Duplicate survey"):
            self.run_export()

    def test_missing_stable_id_rejected_even_when_feature_id_matches_it(self):
        for missing in (None, "", " ", 0):
            with self.subTest(missing=missing):
                self.data["features"][0]["id"] = missing
                self.data["features"][0]["properties"]["odk_submission_id"] = missing
                with self.assertRaisesRegex(ValueError, "Record 1:.*submission"):
                    self.run_export()

    def test_selection_typos_empty_and_group_values_fail(self):
        for fields in ([], [QUESTIONS[0], QUESTIONS[0]], ["/data/unknown"]):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                export_map(self.data, self.mapping, self.form, "outline", fields=fields)
        with self.assertRaises(ValueError):
            self.run_export(identifiers=["__system"])
        group = json.loads(self.data["features"][0]["properties"]["survey_questions"])
        group["grp_bldg"]["building_levels"] = {"hidden": "SECRET"}
        self.data["features"][0]["properties"]["survey_questions"] = json.dumps(group)
        with self.assertRaises(ValueError):
            self.run_export()

    def test_empty_batch_rejected(self):
        with self.assertRaises(ValueError):
            export_map(
                {"type": "FeatureCollection", "features": []},
                self.mapping,
                self.form,
                "outline",
                fields=QUESTIONS[:2],
            )
