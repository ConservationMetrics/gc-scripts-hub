# requirements:
# psycopg2-binary
# requests~=2.32

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import TypedDict

import requests
from psycopg2 import sql

from f.common_logic.db_operations import StructuredDBWriter, conninfo, postgresql
from f.common_logic.file_operations import save_data_to_file
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres


# https://hub.windmill.dev/resource_types/273/gfw_api
class gfw(TypedDict):
    api_key: str


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    gfw: gfw,
    bounding_box: str,
    type_of_alert: str,
    minimum_date: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    storage_path = Path(attachment_root) / db_table_name

    alerts = fetch_alerts_from_gfw(
        gfw["api_key"], bounding_box, type_of_alert, minimum_date
    )

    alerts_geojson = format_alerts_as_geojson(alerts, type_of_alert)

    save_data_to_file(
        alerts_geojson,
        db_table_name,
        storage_path,
        file_type="geojson",
    )

    rel_geojson_path = Path(db_table_name) / f"{db_table_name}.geojson"
    full_geojson_path = Path(attachment_root) / rel_geojson_path

    # Only save to PostgreSQL if the GeoJSON file was created (i.e., there are alerts)
    if full_geojson_path.exists():
        save_geojson_to_postgres(
            db,
            db_table_name,
            rel_geojson_path,
            attachment_root,
            False,  # to not delete the GeoJSON file after its contents are written to the database.
        )
        logger.info(f"GeoJSON data saved to PostgreSQL table [{db_table_name}].")
    else:
        logger.info(
            f"No alerts found, skipping GeoJSON to PostgreSQL save for [{db_table_name}]."
        )

    # Prepare and write metadata
    prepared_metadata = prepare_gfw_metadata(alerts, type_of_alert, minimum_date)

    metadata_table_name = f"{db_table_name}__metadata"
    logger.info(
        f"Writing GFW alerts metadata to database table [{metadata_table_name}]."
    )

    metadata_writer = StructuredDBWriter(
        conninfo(db),
        metadata_table_name,
        predefined_schema=create_gfw_metadata_table,
    )
    metadata_writer.handle_output(prepared_metadata)

    logger.info("GFW alerts metadata saved successfully.")


def fetch_alerts_from_gfw(
    api_key: gfw, bounding_box: str, type_of_alert: str, minimum_date: str
):
    """
    Get alerts from GFW API using the provided API key and bounding box.

    Parameters
    ----------
    api_key : str
        API key for authenticating with the GFW API.
    bounding_box : str
        The bounding box coordinates for the area of interest.
    type_of_alert : str
        The type of alert to fetch from the GFW API.
    minimum_date : str
        The minimum date for filtering alerts.

    # GFW API documentation: https://www.globalforestwatch.org/help/developers/guides/query-data-for-a-custom-geometry/
    # TODO: Figure out a workaround for the maximum allowed payload size of 6291556 bytes

    Returns
    -------
    list
        A list of alerts fetched from the GFW API.
    """
    logger.info("Fetching alerts from GFW API...")
    url = f"https://data-api.globalforestwatch.org/dataset/{type_of_alert}/latest/query"
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
    }
    bbox_array = json.loads(bounding_box)

    data = {
        # Note: we can also provide a GeoJSON feature collection here.
        # The advantage would be that the geometry can be more complex than a simple bounding box.
        # Let's decide whether to do this or not after this script gets some usage.
        "geometry": {"type": "Polygon", "coordinates": bbox_array},
        # Note that VIIRS fire alerts have a different schema than the other alerts.
        "sql": (
            f"SELECT latitude, longitude, "
            f"{'alert__date, confidence__cat' if type_of_alert == 'nasa_viirs_fire_alerts' else f'{type_of_alert}__date, {type_of_alert}__confidence'} "
            f"FROM results WHERE "
            f"{'alert__date' if type_of_alert == 'nasa_viirs_fire_alerts' else f'{type_of_alert}__date'} >= '{minimum_date}'"
        ),
    }

    response = requests.post(url, headers=headers, json=data)

    if not response.ok:
        try:
            error_body = response.json()
            if isinstance(error_body, dict) and "message" in error_body:
                logger.error(f"GFW API Error: {error_body['message']}")
        except (ValueError, json.JSONDecodeError):
            pass

        response.raise_for_status()

    results = response.json().get("data", [])

    logger.info(f"Received {len(results)} alerts from GFW API.")

    return results


def format_alerts_as_geojson(alerts: list, type_of_alert: str):
    """
    Format alerts as GeoJSON.

    Parameters
    ----------
    alerts : list
        A list of alerts to be formatted.
    type_of_alert : str
        The type of alert being processed.

    Returns
    -------
    dict
        A GeoJSON representation of the alerts.
    """
    logger.info("Formatting alerts as GeoJSON.")
    features = []
    for alert in alerts:
        lat = alert["latitude"]
        lon = alert["longitude"]
        # Again, VIIRS fire alerts have a different schema than the other alerts.
        if type_of_alert == "nasa_viirs_fire_alerts":
            date = alert["alert__date"]
            confidence_map = {"n": "nominal", "l": "low", "h": "high"}
            confidence = confidence_map.get(
                alert["confidence__cat"], alert["confidence__cat"]
            )
        else:
            date = alert[f"{type_of_alert}__date"]
            confidence = alert[f"{type_of_alert}__confidence"]
        # GFW alerts do not have IDs. So, let's create a unique id by combining date, latitude, and longitude, and removing non-integer characters.
        id = re.sub(
            r"\D",
            "",
            f"{date}{lat}{lon}",
        )

        feature = {
            "type": "Feature",
            "id": id,
            "geometry": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)],
            },
            "properties": {
                "alert_id": id,
                "alert_type": type_of_alert,
                "confidence": confidence,
                "date_start_t0": date,
                "date_end_t0": date,
                "date_start_t1": date,
                "date_end_t1": date,
                "year_detec": date.split("-")[0],
                "month_detec": date.split("-")[1],
                "data_source": "Global Forest Watch",
            },  # Note: GFW alerts do not have date start and end. So, we set them to the same value.
        }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    logger.info("GeoJSON formatting complete.")
    return geojson


def prepare_gfw_metadata(alerts: list, type_of_alert: str, minimum_date: str):
    """
    Prepare GFW alerts metadata for database storage.

    This function creates metadata records for all months from minimum_date
    to the current month, tracking the alerts found or creating zero-count
    records for months with no alerts. This ensures we track the full detection
    range showing when the algorithm has been running.

    Parameters
    ----------
    alerts : list
        A list of alerts fetched from GFW API.
    type_of_alert : str
        The type of alert being processed (e.g., 'nasa_viirs_fire_alerts', 'gfw_integrated_alerts').
    minimum_date : str
        The minimum date used for the query (format: YYYY-MM-DD).

    Returns
    -------
    list of dict
        A list containing metadata records for all months from minimum_date to current month.
    """
    logger.info("Preparing GFW alerts metadata.")

    # Parse the minimum_date to get start year and month
    date_parts = minimum_date.split("-")
    start_year = int(date_parts[0])
    start_month = int(date_parts[1])

    # Get current date for end range
    current_date = datetime.now()
    end_year = current_date.year
    end_month = current_date.month

    # Map alert types to descriptions
    alert_descriptions = {
        "nasa_viirs_fire_alerts": "fires",
        "gfw_integrated_alerts": "deforestation",
        "glad_alerts": "deforestation",
        "radd_alerts": "deforestation",
        "modis_alerts": "deforestation",
    }

    description_alerts = alert_descriptions.get(type_of_alert, "deforestation")

    # Count alerts by month
    alerts_by_month = {}
    for alert in alerts:
        # Extract date from alert (format varies by alert type)
        if type_of_alert == "nasa_viirs_fire_alerts":
            date_str = alert.get("alert__date")
        else:
            date_str = alert.get(f"{type_of_alert}__date")

        if date_str:
            # Parse date to get year and month
            date_parts = date_str.split("-")
            alert_year = int(date_parts[0])
            alert_month = int(date_parts[1])

            # Only count alerts within our date range
            if (
                alert_year > start_year
                or (alert_year == start_year and alert_month >= start_month)
            ) and (
                alert_year < end_year
                or (alert_year == end_year and alert_month <= end_month)
            ):
                month_key = (alert_year, alert_month)
                alerts_by_month[month_key] = alerts_by_month.get(month_key, 0) + 1

    # Create metadata records for all months from start to current
    metadata_records = []

    current_year = start_year
    current_month = start_month

    while (current_year < end_year) or (
        current_year == end_year and current_month <= end_month
    ):
        # Get alert count for this month
        month_key = (current_year, current_month)
        month_alerts = alerts_by_month.get(month_key, 0)

        metadata_record = {
            "_id": f"{type_of_alert}_{current_year}_{current_month:02d}",
            "month": current_month,
            "year": current_year,
            "total_alerts": month_alerts,
            "description_alerts": description_alerts,
            "type_alert": type_of_alert,
            "data_source": "Global Forest Watch",
        }
        metadata_records.append(metadata_record)

        # Move to next month
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

    total_alerts = sum(alerts_by_month.values())
    logger.info(
        f"Prepared metadata for {len(metadata_records)} months ({start_year}-{start_month:02d} to {end_year}-{end_month:02d}): {total_alerts} total alerts distributed by month"
    )
    return metadata_records


def create_gfw_metadata_table(cursor, table_name):
    """Create the metadata table for GFW alerts."""
    cursor.execute(
        sql.SQL("""
        CREATE TABLE IF NOT EXISTS {metadata_table} (
            _id character varying(100) NOT NULL PRIMARY KEY,
            month bigint,
            year bigint,
            total_alerts bigint,
            description_alerts text,
            type_alert text,
            data_source text
        );
    """).format(metadata_table=sql.Identifier(table_name))
    )
