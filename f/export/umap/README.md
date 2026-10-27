# HOT ODK survey export for uMap

Turn a prepared HOT building-survey export into a `.umap` file. Choose the
questions to show, retain their original form wording and recorded colors, and
choose a point or outline. The script returns two downloads: the map and a JSON
report listing each record's selected answers, styling and optional IDs.

This is a bounded integration for testing. It does not extract from ODK, host a
map, evaluate QGIS styles or form expressions, or implement a FieldTM button.

## Try it locally

From the repository root, using Python 3.11 or later:

```sh
python -m venv .venv
.venv/bin/python -m pip install -r f/export/umap/odk_to_umap.script.lock
.venv/bin/python -m f.export.umap.example.run /tmp/odk-umap-example
```

Import `/tmp/odk-umap-example/survey.umap` into a new uMap using its import panel
and uMap format. The synthetic example has two buildings: cyan with two stories
and purple with four. Both show “Building use: Industrial”. These are invented
records, not partner data. To run the tests: `tox -e umap`.

## Try it in Windmill

A workspace developer must install `f/common_logic/odk_umap` and
`f/export/umap/odk_to_umap`, with their metadata and lock files. An Operator needs
permission to run the installed export script. No database or storage resource
is required by this script.

1. Upload `example/synthetic.geojson`, `example/columns.json` and `example/form.xml`
   into `geojson`, `columns` and `form_xml` respectively.
2. Set `language` to `english(en)`. Add these two entries to `fields`:
   `/data/survey_questions/grp_bldg/building` and
   `/data/survey_questions/grp_bldg/building_levels`.
3. Choose `feature-point` or `outline`. Add `entity_id`, `submission_id` and
   `submission_version_id` to `identifiers` if you want them in the downloads.
4. Run. Download `survey.umap` and `survey.report.json` from the result, then import
   the map into uMap. Check both buildings against the report before saving or
   sharing the map.

If the imported features are outside the current view, open uMap's data browser
and choose **Zoom to layer extent**, then **Bring feature to center** to open a
record's popup.

The file inputs and downloads follow Windmill's
[binary inputs](https://www.windmill.dev/docs/core_concepts/files_binary_data)
and [file results](https://www.windmill.dev/docs/core_concepts/rich_display_rendering).
The callable and result contents have been tested locally. Point and outline
outputs have also been imported into HOT's uMap instance without saving a map.
Installation, file upload controls and downloads still need testing in shared
Windmill; the uMap check did not use a shared Windmill job result.

## Connect prepared survey data

The handoff from extraction is three files, not arbitrary GeoJSON:

- **GeoJSON:** a FeatureCollection with one feature per survey and enriched
  identity properties. The source feature geometry is not used: choose the
  relevant HOT source field explicitly below.
- **Columns:** a JSON object from original names to exported property names, for
  example `{"marker-color": "marker_color"}`. The existing importer can create
  a `__columns` sidecar; this script does not query or export that table.
- **XForm:** the original published form XML for the same form version and
  language. A formatted XML viewer's copied text is not a substitute.

Required original property names are `dataset_name` (form ID),
`odk_form_version`, `odk_submission_id`, `odk_submission_version_id`,
`odk_declared_entity_id`, `odk_entity_dataset`, `meta-instanceID`, `feature`,
`fill`, `stroke`, `marker-color` and `stroke-width`, plus the selected answers and
geometry fields. The `feature` property may be empty, but when present its value
must match the declared entity ID. Each GeoJSON feature's `id` must match the
stable submission ID; `meta-instanceID` must match its version ID.

The identity enrichment is a separate proposed change to the ODK connector.
An unchanged connector export is not sufficient. These input properties must
be supplied by that enrichment or an equivalent verified preparation step.
This exporter does not invent IDs or infer them from coordinates.

Flattened CSV properties and JSON-valued API groups are supported. For example,
`meta-instanceID` can resolve from a mapped `meta` object, and question paths
resolve from a mapped `survey_questions` object. API aliases `marker_color` and
`stroke_width` are accepted; contradictory alias values are rejected.

| Choice | Source |
| --- | --- |
| `feature-point` | `xlocation`, an ODK latitude/longitude/altitude/accuracy string |
| `survey-point` | `survey_questions-grp_loc-geopoint` with Latitude, Longitude, Altitude and Accuracy components |
| `outline` | `survey_questions-grp_loc-building_outline`, an ODK coordinate string or a GeoJSON Polygon with one ring |

There is no fallback between sources. Altitude and accuracy do not appear in the
two-dimensional map. A declared building ID is not proof that ODK has applied
an entity update. Distinct surveys of one building stay separate; duplicate
survey IDs are rejected rather than silently selecting a version.

## Limits and data handling

Only selected scalar answers and selected IDs enter either download, alongside
geometry and presentation. IDs are validated even when excluded. Wording comes
from the matching form's translation and internal choice lists; colors come from
the recorded source values. This does not recalculate styling rules.

Supported: one form version per batch, plain single-line labels, scalar answers,
internal single-select lists, points and single-ring outlines. Unsupported:
repeats, multiselects, external choices, media, mixed form versions and complex
geometry. Unsupported or inconsistent records reject the entire batch. Outline
validation checks closure, coordinate range and nonzero area; it is not a full
topology validator. Uploads and each decoded output are limited to 5 MiB; the
workspace may impose a smaller job-result limit.

Windmill receives the full input files and may retain them and job errors.
Field selection restricts the downloads, not access to job inputs. Use an
approved workspace and approved data; never put credentials in these files.
Saving a map in uMap has separate visibility settings that the user must review.
