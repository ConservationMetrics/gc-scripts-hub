"""Build uMap files from prepared HOT ODK surveys and their original form."""

import html
import json
import math
import re
from uuid import UUID

from defusedxml import ElementTree as ET

NS = {"x": "http://www.w3.org/2002/xforms"}


def plain(text):
    if not isinstance(text, str) or any(c in text for c in "{}<>*[]\r\n"):
        raise ValueError("This exporter requires plain, single-line display text")
    return html.escape(text, quote=True)


def read_form(path, language, field_paths):
    """Read selected question labels and internal choices from a matching XForm.

    Parameters
    ----------
    path : path-like or binary file-like
        Original published XForm XML.
    language : str
        Exact translation language name in the form.
    field_paths : list of str
        Absolute question paths under /data/.

    Returns
    -------
    dict
        Form identity, selected labels and choice wording.
    """
    root = ET.parse(path).getroot()
    instance = root.find(".//x:model/x:instance", NS)
    if instance is None or not len(instance):
        raise ValueError("Form has no primary instance")
    form = instance[0]
    translation = next(
        (t for t in root.findall(".//x:translation", NS) if t.get("lang") == language),
        None,
    )
    if translation is None:
        raise ValueError(f"Form has no language {language!r}")
    texts = {}
    for element in translation.findall("x:text", NS):
        value = next(
            (v for v in element.findall("x:value", NS) if not v.get("form")), None
        )
        if value is not None and not list(value):
            texts[element.attrib["id"]] = value.text or ""
    fields = []
    for path in field_paths:
        if not re.fullmatch(r"/data/(?:[A-Za-z_][\w.-]*/)*[A-Za-z_][\w.-]*", path):
            raise ValueError("Expected an absolute question path under /data/")
        if root.find(f'.//x:select[@ref="{path}"]', NS) is not None:
            raise ValueError(f"Multi-select questions are not supported yet: {path}")
        if path + ":label" not in texts:
            raise ValueError(f"Missing supported question label: {path}")
        label = texts[path + ":label"]
        plain(label)
        choices = {}
        control = root.find(f'.//x:select1[@ref="{path}"]', NS)
        if control is not None:
            itemset = control.find("x:itemset", NS)
            match = re.fullmatch(
                r"instance\('([^']+)'\)/root/item",
                itemset.attrib["nodeset"] if itemset is not None else "",
            )
            if not match:
                raise ValueError(f"Unsupported choice source: {path}")
            instance = root.find(f'.//x:instance[@id="{match[1]}"]', NS)
            if instance is None:
                raise ValueError(f"Missing choice list for question: {path}")
            for item in instance.findall("x:root/x:item", NS):
                code = item.findtext("x:name", namespaces=NS)
                wording_id = item.findtext("x:itextId", namespaces=NS)
                if not code or wording_id not in texts:
                    raise ValueError(
                        f"Missing choice code or translated label for question: {path}"
                    )
                wording = texts[wording_id]
                plain(wording)
                choices[code] = wording
            if not choices:
                raise ValueError(f"Empty internal choice list for question: {path}")
        fields.append({"path": path, "label": label, "choices": choices})
    entity = form.find("./{*}meta/{*}entity")
    if (
        not form.get("id")
        or not form.get("version")
        or entity is None
        or not entity.get("dataset")
    ):
        raise ValueError("Form requires its ID, version and entity dataset")
    return {
        "id": form.attrib["id"],
        "version": form.attrib["version"],
        "entity_dataset": entity.get("dataset"),
        "language": language,
        "fields": fields,
    }


def position(value):
    if not isinstance(value, str):
        raise ValueError("Missing ODK coordinate string")
    parts = value.split()
    if len(parts) != 4:
        raise ValueError("Expected ODK latitude longitude altitude accuracy")
    lat, lon, altitude, accuracy = map(float, parts)
    if not all(math.isfinite(n) for n in (lat, lon, altitude, accuracy)):
        raise ValueError("Coordinates must be finite")
    if not (-90 <= lat <= 90 and -180 <= lon <= 180) or accuracy < 0:
        raise ValueError("Coordinate outside supported range")
    return [lon, lat]


def geometry_for(get, source):
    if source == "feature-point":
        value = get("xlocation")
        return {"type": "Point", "coordinates": position(value)}
    if source == "survey-point":
        prefix = "survey_questions-grp_loc-geopoint"
        value = " ".join(
            str(get(prefix + "-" + suffix))
            for suffix in ("Latitude", "Longitude", "Altitude", "Accuracy")
        )
        return {"type": "Point", "coordinates": position(value)}
    if source != "outline":
        raise ValueError("Choose feature-point, survey-point or outline explicitly")
    value = get("survey_questions-grp_loc-building_outline")
    if isinstance(value, dict):
        coordinates = value.get("coordinates")
        if value.get("type") != "Polygon" or not coordinates or len(coordinates) != 1:
            raise ValueError("This exporter requires one polygon ring")
        points = []
        for point in coordinates[0]:
            if len(point) not in (2, 3):
                raise ValueError(
                    "Expected GeoJSON longitude latitude and optional altitude"
                )
            points.append(
                f"{point[1]} {point[0]} {point[2] if len(point) == 3 else 0} 0"
            )
        value = ";".join(points)
    if not isinstance(value, str) or not value.strip():
        raise ValueError("No building outline; no point fallback was selected")
    ring = [position(part.strip()) for part in value.split(";")]
    if len(ring) < 4 or ring[0] != ring[-1] or len(set(map(tuple, ring))) < 3:
        raise ValueError(
            "Outline must contain a closed ring with three distinct vertices"
        )
    area = sum(a[0] * b[1] - b[0] * a[1] for a, b in zip(ring, ring[1:]))
    if abs(area) < 1e-16:
        raise ValueError("Outline has zero signed area")
    return {"type": "Polygon", "coordinates": [ring]}


def color(value):
    if not isinstance(value, str) or not re.fullmatch(r"#[0-9a-fA-F]{6}", value):
        raise ValueError("This exporter supports explicit six-digit source colors")
    return value


def export_value(props, mapping, name):
    """Read either a CSV column or an API group preserved as a JSON column."""
    alias = {"marker-color": "marker_color", "stroke-width": "stroke_width"}.get(name)
    if alias and alias in mapping and mapping[alias] in props:
        value = props[mapping[alias]]
        if name in mapping and mapping[name] in props and props[mapping[name]] != value:
            raise ValueError(f"Conflicting CSV/API values for {name}")
        return value
    if name in mapping and mapping[name] in props:
        return props[mapping[name]]
    parts = name.split("-")
    if len(parts) > 1 and parts[0] in mapping:
        value = props.get(mapping[parts[0]])
        if isinstance(value, str):
            value = json.loads(value)
        for part in parts[1:]:
            if not isinstance(value, dict) or part not in value:
                raise ValueError(f"Missing mapped source field: {name}")
            value = value[part]
        return value
    raise ValueError(f"Missing mapped source field: {name}")


def export_identity(get, feature_id, form):
    stable_id = get("odk_submission_id")
    version_id = get("odk_submission_version_id")
    entity_id = get("odk_declared_entity_id")
    if any(
        not isinstance(value, str) or not value.strip()
        for value in (stable_id, entity_id, version_id)
    ):
        raise ValueError("Export requires entity, submission and version IDs")
    UUID(entity_id)
    if stable_id != feature_id or version_id != get("meta-instanceID"):
        raise ValueError("Export identity columns disagree with the survey")
    if get("odk_entity_dataset") != form["entity_dataset"]:
        raise ValueError("Export entity dataset does not match the form")
    if get("odk_form_version") != form["version"]:
        raise ValueError("Export and form versions disagree")
    selected_feature = get("feature")
    if selected_feature and selected_feature != entity_id:
        raise ValueError("Selected feature and XML entity ID disagree")
    return {
        "id": stable_id,
        "version_id": version_id,
        "entity_id": entity_id,
        "entity_dataset": get("odk_entity_dataset"),
    }


def _convert_record(feature, mapping, form, source):
    """Validate one source record and prepare its selected presentation."""
    if not isinstance(feature, dict) or feature.get("type") != "Feature":
        raise ValueError("Expected a GeoJSON Feature")
    props = feature.get("properties")
    if not isinstance(props, dict):
        raise ValueError("Feature properties must be an object")
    display_props = {}

    def get(name):
        return export_value(props, mapping, name)

    version = (
        get("FormVersion") if "FormVersion" in mapping else get("odk_form_version")
    )
    if get("dataset_name") != form["id"] or version != form["version"]:
        raise ValueError("The form ID/version does not match this submission")
    submission = export_identity(get, feature.get("id"), form)
    geometry = geometry_for(get, source)
    fill, stroke, marker = (color(get(k)) for k in ("fill", "stroke", "marker-color"))
    weight = float(get("stroke-width"))
    if not math.isfinite(weight) or weight < 0:
        raise ValueError("Invalid source stroke width")
    style = (
        {"color": marker, "iconClass": "Circle", "opacity": 1}
        if source != "outline"
        else {
            "color": stroke,
            "fillColor": fill,
            "weight": weight,
            "fill": True,
            "stroke": True,
            "opacity": 1,
            "fillOpacity": 1,
        }
    )
    display_props["_umap_options"] = style
    rows, display = [], []
    for index, field in enumerate(form["fields"]):
        csv_key = field["path"].removeprefix("/data/").replace("/", "-")
        answer = get(csv_key)
        if answer is None:
            wording = "—"
        elif field["choices"]:
            if answer not in field["choices"]:
                raise ValueError(f"Unknown choice code {answer!r} for {csv_key}")
            wording = field["choices"][answer]
        else:
            wording = str(answer)
        display_key = f"_odk_display_{index}"
        display_props[display_key] = plain(wording)
        rows.append(
            f'<div style="margin:0 0 5px">{plain(field["label"])}: '
            f"<strong>{{{display_key}}}</strong></div>"
        )
        display.append({"label": field["label"], "raw": answer, "display": wording})
    popup = (
        '<div style="text-align:left;white-space:normal;line-height:1.35;'
        'width:250px;max-width:calc(100vw - 90px)">' + "".join(rows) + "</div>"
    )
    identity = {
        "submission_id": submission["id"],
        "submission_version_id": submission["version_id"],
        "entity_id": submission["entity_id"],
        "entity_dataset": submission["entity_dataset"],
    }
    return geometry, display_props, display, popup, identity


IDENTIFIERS = ("entity_id", "submission_id", "submission_version_id", "entity_dataset")


def export_map(data, mapping, form, source, *, fields, identifiers=()):
    """Build a map containing only selected answers and identifiers.

    Parameters
    ----------
    data : dict
        Enriched HOT ODK export as a GeoJSON FeatureCollection.
    mapping : dict
        Original column name to exported property name.
    form : dict
        Result of read_form for the same form version.
    source : str
        Explicit feature-point, survey-point or outline geometry choice.
    fields : list of str
        Question paths to include in the popup and properties.
    identifiers : sequence of str, optional
        Identifiers to include. All identifiers are checked even when omitted.

    Returns
    -------
    tuple of dict
        uMap document and verification report. Invalid records reject the batch.
    """
    features = data.get("features")
    if (
        data.get("type") != "FeatureCollection"
        or not isinstance(features, list)
        or not features
    ):
        raise ValueError("Export requires a nonempty FeatureCollection")
    if not fields or len(set(fields)) != len(fields):
        raise ValueError("Choose at least one question, without duplicates")
    available = {field["path"]: field for field in form["fields"]}
    if any(path not in available for path in fields):
        raise ValueError("Selected questions must exist in the supplied form")
    if len(set(identifiers)) != len(identifiers) or any(
        key not in IDENTIFIERS for key in identifiers
    ):
        raise ValueError("Choose supported identifiers, without duplicates")
    selected_form = {**form, "fields": [available[path] for path in fields]}
    output_features, reports, positions, seen = [], [], [], set()
    popup = None
    for index, feature in enumerate(features):
        try:
            geometry, props, display, popup, identity = _convert_record(
                feature, mapping, selected_form, source
            )
            stable_id = identity["submission_id"]
            if stable_id in seen:
                raise ValueError(
                    "Duplicate survey ID; choose the intended version before export"
                )
            seen.add(stable_id)
            # Copy individual answers; groups may contain unselected data.
            for path, answer in zip(fields, display):
                value = answer["raw"]
                if value is not None and not isinstance(value, (str, int, float, bool)):
                    raise ValueError(
                        "Selected answers must be scalar values, not whole groups"
                    )
                if isinstance(value, float) and not math.isfinite(value):
                    raise ValueError("Selected numeric answers must be finite")
                props[path] = value
            chosen_ids = {key: identity[key] for key in identifiers}
            props.update({f"_odk_{key}": value for key, value in chosen_ids.items()})
            result = {
                "type": "Feature",
                "geometry": geometry,
                "properties": props,
            }
            if "submission_id" in chosen_ids:
                result["id"] = chosen_ids["submission_id"]
            output_features.append(result)
            coords = result["geometry"]["coordinates"]
            positions.extend(coords[0] if source == "outline" else [coords])
            reports.append(
                {
                    "record": index + 1,
                    "identifiers": chosen_ids,
                    "display": display,
                    "style": props["_umap_options"],
                }
            )
        except (ValueError, KeyError, TypeError) as error:
            raise ValueError(f"Record {index + 1}: {error}") from error
    west, east = min(p[0] for p in positions), max(p[0] for p in positions)
    south, north = min(p[1] for p in positions), max(p[1] for p in positions)

    def mercator_y(latitude):
        latitude = math.radians(max(-85.05112878, min(85.05112878, latitude)))
        return math.log(math.tan(math.pi / 4 + latitude / 2))

    bottom, top = mercator_y(south), mercator_y(north)
    latitude = math.degrees(2 * math.atan(math.exp((bottom + top) / 2)) - math.pi / 2)
    fraction = max((east - west) / 360, (top - bottom) / (2 * math.pi))
    # Keep the full extent within 256 pixels.
    zoom = 22 if fraction == 0 else max(0, min(22, math.floor(math.log2(1 / fraction))))
    document = {
        "type": "umap",
        "geometry": {"type": "Point", "coordinates": [(west + east) / 2, latitude]},
        "properties": {"name": f"ODK — {source}", "zoom": zoom},
        "layers": [
            {
                "type": "FeatureCollection",
                "features": output_features,
                "properties": {
                    "name": source,
                    "type": "Default",
                    "displayOnLoad": True,
                    "browsable": True,
                    "showLabel": False,
                    "popupTemplate": "Default",
                    "popupContentTemplate": popup,
                },
            }
        ],
    }
    report = {
        "feature_count": len(output_features),
        "selected_fields": list(fields),
        "selected_identifiers": list(identifiers),
        "geometry_source": source,
        "bounds": [west, south, east, north],
        "records": reports,
        "note": "Only selected answers and IDs are exported, "
        "plus geometry and presentation. "
        "Unselected source properties are excluded from both map and report.",
    }
    return document, report
