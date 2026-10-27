"""Run the same entry point locally with synthetic inputs; no account needed."""

import base64
import sys
from pathlib import Path

from f.export.umap.odk_to_umap import main

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m f.export.umap.example.run OUTPUT_DIRECTORY")
    source = Path(__file__).parent
    result = main(
        geojson=(source / "synthetic.geojson").read_bytes(),
        columns=(source / "columns.json").read_bytes(),
        form_xml=(source / "form.xml").read_bytes(),
        language="english(en)",
        fields=[
            "/data/survey_questions/grp_bldg/building",
            "/data/survey_questions/grp_bldg/building_levels",
        ],
        geometry="outline",
        identifiers=["entity_id", "submission_id", "submission_version_id"],
    )
    destination = Path(sys.argv[1])
    destination.mkdir(parents=True, exist_ok=True)
    for item in result["render_all"]:
        file = item["file"]
        path = destination / file["filename"]
        path.write_bytes(base64.b64decode(file["content"], validate=True))
        print(path)
