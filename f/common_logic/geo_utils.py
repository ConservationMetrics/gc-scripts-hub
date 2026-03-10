import json
import logging
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def geojson_to_line_delimited(source_path: Path) -> Path:
    """
    Convert a standard GeoJSON file into line-delimited GeoJSON (one feature per line).
    """
    logger.info("Converting GeoJSON file %s to line-delimited format.", source_path)

    with source_path.open(encoding="utf-8") as src:
        data = json.load(src)

    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        features = data.get("features", [])
    else:
        # Fallback: treat the whole object as a single feature/geometry line
        features = [data]

    with tempfile.NamedTemporaryFile(
        "w",
        suffix=".geojson.ld",
        encoding="utf-8",
        delete=False,
    ) as tmp:
        ld_path = Path(tmp.name)
        for feature in features:
            json.dump(feature, tmp, ensure_ascii=False, separators=(",", ":"))
            tmp.write("\n")

    logger.debug(
        "Finished writing %d line-delimited GeoJSON feature(s) to temporary file %s.",
        len(features),
        ld_path,
    )

    return ld_path
