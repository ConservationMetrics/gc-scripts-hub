# requirements:
# psycopg2-binary
# lxml

import logging
from pathlib import Path

from f.common_logic.db_operations import postgresql
from f.common_logic.file_operations import save_data_to_file
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    smart_patrols_path: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    """
    Parse SMART patrol XML and save observations to database as GeoJSON.

    Parameters
    ----------
    smart_patrols_path : str
        The path (in attachment root) to the SMART patrols XML file to import.
    db : postgresql
        Database connection configuration.
    db_table_name : str
        The name of the database table where observations will be stored.
    attachment_root : str
        Root directory for persistent storage.
    """
    # Construct full path to XML file
    xml_path = Path(attachment_root) / smart_patrols_path

    if not xml_path.exists():
        raise FileNotFoundError(f"SMART patrol XML file not found: {xml_path}")

    logger.info(f"Reading SMART patrol XML from {xml_path}")

    # Parse XML and extract observations as GeoJSON
    observations_geojson = parse_smart_patrol_xml(xml_path)

    num_observations = len(observations_geojson.get("features", []))
    logger.info(f"Extracted {num_observations} observations from SMART patrol XML")

    # Create project directory in datalake
    project_dir = Path(attachment_root) / db_table_name
    project_dir.mkdir(parents=True, exist_ok=True)

    # Save raw XML file to datalake
    xml_save_path = project_dir / xml_path.name
    xml_save_path.write_text(xml_path.read_text())
    logger.info(f"Saved raw XML file to: {xml_save_path}")

    # Apply transformation to add data_source
    observations_geojson = transform_smart_patrol_data(observations_geojson)

    # Save GeoJSON file to datalake
    if observations_geojson.get("features"):
        save_data_to_file(
            observations_geojson,
            db_table_name,
            project_dir,
            file_type="geojson",
        )
        logger.info(
            f"Saved GeoJSON file to: {project_dir / f'{db_table_name}.geojson'}"
        )

        # Write to database using geojson_to_postgres
        geojson_rel_path = Path(db_table_name) / f"{db_table_name}.geojson"
        save_geojson_to_postgres(
            db=db,
            db_table_name=db_table_name,
            geojson_path=str(geojson_rel_path),
            attachment_root=attachment_root,
            delete_geojson_file=False,
        )
        logger.info(
            f"SMART patrol observations successfully written to database table: [{db_table_name}]"
        )
    else:
        logger.warning("No observations found in SMART patrol XML")


def transform_smart_patrol_data(data: dict, dataset_name: str = None) -> dict:
    """
    Apply SMART-specific transformations to patrol data.

    This transformation adds the data_source field to identify the data as coming
    from SMART Conservation Software.

    Parameters
    ----------
    data : dict
        GeoJSON FeatureCollection with SMART patrol observations.
    dataset_name : str, optional
        Human-readable name of the dataset (currently unused, included for
        consistency with other transformation functions).

    Returns
    -------
    dict
        Transformed GeoJSON with data_source field added to all feature properties.
    """
    if "features" in data:
        for feature in data["features"]:
            if "properties" not in feature:
                feature["properties"] = {}
            feature["properties"]["data_source"] = "SMART"
    return data


def parse_smart_patrol_xml(xml_path: Path) -> dict:
    """
    Parse SMART patrol XML and extract observations with full context as GeoJSON.

    Parameters
    ----------
    xml_path : Path
        Path to the SMART patrol XML file.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection with observation features containing all contextual information.
    """
    # Use the parsing logic from data_conversion to avoid duplicating code
    # and to ensure the app can use it without database dependencies
    from f.common_logic.data_conversion import smart_xml_to_geojson

    return smart_xml_to_geojson(xml_path)
