"""Return map and report downloads from prepared HOT ODK survey files."""

import base64
import json
from io import BytesIO
from typing import Literal

from f.common_logic.odk_umap import export_map, read_form


def main(
    geojson: bytes,
    columns: bytes,
    form_xml: bytes,
    language: str,
    fields: list[str],
    geometry: Literal["feature-point", "survey-point", "outline"],
    identifiers: list[str] | None = None,
):
    """Convert uploaded files into Windmill file results without publishing a map.

    Parameters
    ----------
    geojson : bytes
        Enriched HOT ODK GeoJSON export, uploaded as a file.
    columns : bytes
        JSON object mapping original columns to exported properties.
    form_xml : bytes
        Matching published XForm XML, uploaded as a file.
    language : str
        Exact translation name, for example english(en).
    fields : list of str
        Absolute question paths to include; no fields are selected implicitly.
    geometry : {"feature-point", "survey-point", "outline"}
        Geometry to use for every record. There is no silent fallback.
    identifiers : list of str, optional
        Identity fields to include in the downloads; defaults to none.

    Returns
    -------
    dict
        Windmill render_all result containing a map and report file download.
    """
    # File results avoid requiring a workspace storage resource.
    for name, value in (
        ("geojson", geojson),
        ("columns", columns),
        ("form_xml", form_xml),
    ):
        if not isinstance(value, bytes) or not value or len(value) > 5 * 1024 * 1024:
            raise ValueError(f"{name} must be a nonempty file of at most 5 MiB")
    data, mapping = json.loads(geojson), json.loads(columns)
    if not isinstance(data, dict) or not isinstance(mapping, dict):
        raise ValueError("GeoJSON and columns must be JSON objects")
    if any(
        not isinstance(k, str) or not isinstance(v, str) for k, v in mapping.items()
    ):
        raise ValueError("Columns must map original names to exported property names")
    if (
        not isinstance(fields, list)
        or not fields
        or any(not isinstance(p, str) for p in fields)
    ):
        raise ValueError("Choose at least one question path")
    if identifiers is not None and (
        not isinstance(identifiers, list)
        or any(not isinstance(k, str) for k in identifiers)
    ):
        raise ValueError("Identifiers must be a list of names")
    form = read_form(BytesIO(form_xml), language, fields)
    document, report = export_map(
        data, mapping, form, geometry, fields=fields, identifiers=identifiers or ()
    )
    downloads = []
    for filename, value in (("survey.umap", document), ("survey.report.json", report)):
        content = (
            json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2) + "\n"
        ).encode()
        if len(content) > 5 * 1024 * 1024:
            raise ValueError("Output exceeds 5 MiB; choose a smaller batch")
        downloads.append(
            {
                "file": {
                    "filename": filename,
                    "content": base64.b64encode(content).decode(),
                }
            }
        )
    return {"render_all": downloads}
