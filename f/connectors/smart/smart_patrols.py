# requirements:
# psycopg2-binary
# lxml

import hashlib
import logging
from pathlib import Path

from lxml import etree

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    smart_patrols_path: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    """
    Parse SMART patrol XML and save observations to database.

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

    # Parse XML and extract observations
    observations = parse_smart_patrol_xml(xml_path)

    logger.info(f"Extracted {len(observations)} observations from SMART patrol XML")

    # Save XML to project folder
    save_path = Path(attachment_root) / db_table_name
    save_path.mkdir(parents=True, exist_ok=True)
    xml_save_path = save_path / xml_path.name
    xml_save_path.write_text(xml_path.read_text())
    logger.info(f"Saved XML file to: {xml_save_path}")

    # Write observations to database
    if observations:
        db_writer = StructuredDBWriter(
            conninfo(db),
            db_table_name,
            use_mapping_table=False,
            reverse_properties_separated_by=None,
        )
        db_writer.handle_output(observations)
        logger.info(
            f"SMART patrol observations successfully written to database table: [{db_table_name}]"
        )
    else:
        logger.warning("No observations found in SMART patrol XML")


def parse_smart_patrol_xml(xml_path: Path) -> list[dict]:
    """
    Parse SMART patrol XML and extract observations with full context.

    Parameters
    ----------
    xml_path : Path
        Path to the SMART patrol XML file.

    Returns
    -------
    list[dict]
        A list of observations with all contextual information.
    """
    tree = etree.parse(str(xml_path))
    root = tree.getroot()

    # Define namespace
    ns = {"ns2": "http://www.smartconservationsoftware.org/xml/1.3/patrol"}

    observations = []

    # Extract patrol-level information
    patrol_id = root.get("id")
    patrol_type = root.get("patrolType")
    patrol_start_date = root.get("startDate")
    patrol_end_date = root.get("endDate")
    patrol_is_armed = root.get("isArmed")

    patrol_description = None
    objective_elem = root.find("ns2:objective/ns2:description", ns)
    if objective_elem is not None and objective_elem.text:
        patrol_description = objective_elem.text

    patrol_team = None
    team_elem = root.find("ns2:team", ns)
    if team_elem is not None:
        patrol_team = team_elem.get("value")

    patrol_comment = None
    comment_elem = root.find("ns2:comment", ns)
    if comment_elem is not None and comment_elem.text:
        patrol_comment = comment_elem.text

    # Process each leg
    for leg in root.findall("ns2:legs", ns):
        leg_id = leg.get("id")
        leg_start_date = leg.get("startDate")
        leg_end_date = leg.get("endDate")

        transport_type = None
        transport_elem = leg.find("ns2:transportType", ns)
        if transport_elem is not None:
            transport_type = transport_elem.get("value")

        # Aggregate members
        members_list = []
        for member in leg.findall("ns2:members", ns):
            given_name = member.get("givenName", "")
            family_name = member.get("familyName", "")
            if given_name or family_name:
                members_list.append(f"{given_name} {family_name}".strip())
        members = ", ".join(members_list) if members_list else None

        mandate = None
        mandate_elem = leg.find("ns2:mandate", ns)
        if mandate_elem is not None:
            mandate = mandate_elem.get("value")

        # Process each day
        for day in leg.findall("ns2:days", ns):
            day_date = day.get("date")
            day_start_time = day.get("startTime")
            day_end_time = day.get("endTime")
            day_rest_minutes = day.get("restMinutes")

            # Process each waypoint
            for waypoint in day.findall("ns2:waypoints", ns):
                waypoint_id = waypoint.get("id")
                waypoint_x = waypoint.get("x")  # longitude
                waypoint_y = waypoint.get("y")  # latitude
                waypoint_time = waypoint.get("time")

                # Process each observation group
                groups = waypoint.find("ns2:groups", ns)
                if groups is not None:
                    # Counter for multiple observations of same category at same waypoint
                    observation_counter = {}
                    for observation in groups.findall("ns2:observations", ns):
                        category_key = observation.get("categoryKey")

                        # Track observation count for this category at this waypoint
                        observation_counter[category_key] = (
                            observation_counter.get(category_key, 0) + 1
                        )
                        obs_seq = observation_counter[category_key]

                        # Generate a unique and deterministic ID for this observation
                        # Based on patrol_id, waypoint_id, category_key, and sequence number
                        id_string = (
                            f"{patrol_id}_{waypoint_id}_{category_key}_{obs_seq}"
                        )
                        obs_id = hashlib.md5(id_string.encode()).hexdigest()

                        # Build observation dict with all context
                        obs_dict = {
                            "_id": obs_id,
                            # Patrol-level info
                            "patrol_id": patrol_id,
                            "patrol_type": patrol_type,
                            "patrol_start_date": patrol_start_date,
                            "patrol_end_date": patrol_end_date,
                            "patrol_is_armed": patrol_is_armed,
                            "patrol_description": patrol_description,
                            "patrol_team": patrol_team,
                            "patrol_comment": patrol_comment,
                            # Leg-level info
                            "leg_id": leg_id,
                            "leg_start_date": leg_start_date,
                            "leg_end_date": leg_end_date,
                            "leg_transport_type": transport_type,
                            "leg_members": members,
                            "leg_mandate": mandate,
                            # Day-level info
                            "day_date": day_date,
                            "day_start_time": day_start_time,
                            "day_end_time": day_end_time,
                            "day_rest_minutes": day_rest_minutes,
                            # Waypoint-level info
                            "waypoint_id": waypoint_id,
                            "waypoint_x": waypoint_x,
                            "waypoint_y": waypoint_y,
                            "waypoint_time": waypoint_time,
                            # Observation-level info
                            "category": category_key,
                            # Geometry fields for GeoJSON compliance
                            "g__type": "Point",
                            "g__coordinates": [float(waypoint_x), float(waypoint_y)]
                            if waypoint_x and waypoint_y
                            else None,
                            # Add data source
                            "data_source": "SMART",
                        }

                        # Extract attributes
                        for attribute in observation.findall("ns2:attributes", ns):
                            attr_key = attribute.get("attributeKey")

                            # Check for different value types
                            item_key_elem = attribute.find("ns2:itemKey", ns)
                            d_value_elem = attribute.find("ns2:dValue", ns)
                            b_value_elem = attribute.find("ns2:bValue", ns)
                            s_value_elem = attribute.find("ns2:sValue", ns)

                            if item_key_elem is not None and item_key_elem.text:
                                obs_dict[attr_key] = item_key_elem.text
                            elif d_value_elem is not None and d_value_elem.text:
                                obs_dict[attr_key] = float(d_value_elem.text)
                            elif b_value_elem is not None and b_value_elem.text:
                                obs_dict[attr_key] = b_value_elem.text.lower() == "true"
                            elif s_value_elem is not None and s_value_elem.text:
                                obs_dict[attr_key] = s_value_elem.text

                        observations.append(obs_dict)

    return observations
