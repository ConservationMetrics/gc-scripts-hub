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

from f.common_logic.date_utils import calculate_cutoff_date
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
    max_months_lookback: int,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    storage_path = Path(attachment_root) / db_table_name

    alerts = fetch_alerts_from_gfw(
        gfw["api_key"], bounding_box, type_of_alert, max_months_lookback
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
    prepared_metadata = prepare_gfw_metadata(alerts, type_of_alert, max_months_lookback)

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
    api_key: gfw,
    bounding_box: str,
    type_of_alert: str,
    max_months_lookback: int,
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
    max_months_lookback : int
        Number of months to look back when fetching alerts.

    # GFW API documentation: https://www.globalforestwatch.org/help/developers/guides/query-data-for-a-custom-geometry/
    # TODO: Figure out a workaround for the maximum allowed payload size of 6291556 bytes

    Returns
    -------
    list
        A list of alerts fetched from the GFW API.
    """
    # Calculate start date from lookback period
    cutoff_date = calculate_cutoff_date(max_months_lookback)
    cutoff_year, cutoff_month = cutoff_date
    start_date = f"{cutoff_year}-{cutoff_month:02d}-01"

    logger.info(
        f"Fetching alerts from GFW API for last {max_months_lookback} months (starting from {start_date})..."
    )
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
            f"{'alert__date' if type_of_alert == 'nasa_viirs_fire_alerts' else f'{type_of_alert}__date'} >= '{start_date}'"
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


def prepare_gfw_metadata(alerts: list, type_of_alert: str, max_months_lookback: int):
    """
    Prepare GFW alerts metadata for database storage.

    This function creates metadata records for all days from the lookback period
    to the current date, tracking the alerts found or creating zero-count
    records for days with no alerts. This ensures we track the full detection
    range showing when the algorithm has been running.

    Parameters
    ----------
    alerts : list
        A list of alerts fetched from GFW API.
    type_of_alert : str
        The type of alert being processed (e.g., 'nasa_viirs_fire_alerts', 'gfw_integrated_alerts').
    max_months_lookback : int
        Number of months to look back when processing metadata.

    Returns
    -------
    list of dict
        A list containing metadata records for all days from the lookback period to current date.
    """
    logger.info("Preparing GFW alerts metadata.")

    # Calculate start date from lookback period
    cutoff_date = calculate_cutoff_date(max_months_lookback)
    start_year, start_month = cutoff_date
    start_day = 1  # Start from the first day of the month

    # Get current date for end range
    current_date = datetime.now()
    end_year = current_date.year
    end_month = current_date.month
    end_day = current_date.day

    # Map alert types to descriptions
    alert_descriptions = {
        "nasa_viirs_fire_alerts": "fires",
        "gfw_integrated_alerts": "deforestation",
        "glad_alerts": "deforestation",
        "radd_alerts": "deforestation",
        "modis_alerts": "deforestation",
    }

    description_alerts = alert_descriptions.get(type_of_alert, "deforestation")

    # Count alerts by day
    alerts_by_day = {}
    for alert in alerts:
        # Extract date from alert (field name is different for VIIRS fire alerts)
        if type_of_alert == "nasa_viirs_fire_alerts":
            date_str = alert.get("alert__date")
        else:
            date_str = alert.get(f"{type_of_alert}__date")

        if date_str:
            # Parse date to get year, month, and day
            date_parts = date_str.split("-")
            alert_year = int(date_parts[0])
            alert_month = int(date_parts[1])
            alert_day = int(date_parts[2])

            # Only count alerts within our date range
            if (
                alert_year > start_year
                or (alert_year == start_year and alert_month > start_month)
                or (
                    alert_year == start_year
                    and alert_month == start_month
                    and alert_day >= start_day
                )
            ) and (
                alert_year < end_year
                or (alert_year == end_year and alert_month < end_month)
                or (
                    alert_year == end_year
                    and alert_month == end_month
                    and alert_day <= end_day
                )
            ):
                day_key = (alert_year, alert_month, alert_day)
                alerts_by_day[day_key] = alerts_by_day.get(day_key, 0) + 1

    # Create metadata records for all days from start to current
    metadata_records = []

    current_year = start_year
    current_month = start_month
    current_day = start_day

    while (
        (current_year < end_year)
        or (current_year == end_year and current_month < end_month)
        or (
            current_year == end_year
            and current_month == end_month
            and current_day <= end_day
        )
    ):
        # Get alert count for this day
        day_key = (current_year, current_month, current_day)
        day_alerts = alerts_by_day.get(day_key, 0)

        metadata_record = {
            "_id": f"{type_of_alert}_{current_year}_{current_month:02d}_{current_day:02d}",
            "month": current_month,
            "year": current_year,
            "day": current_day,
            "total_alerts": day_alerts,
            "description_alerts": description_alerts,
            "type_alert": type_of_alert,
            "data_source": "Global Forest Watch",
        }
        metadata_records.append(metadata_record)

        # Move to next day
        current_day += 1
        if (
            current_day > 31
            or (current_day > 30 and current_month in [4, 6, 9, 11])
            or (current_day > 29 and current_month == 2)
            or (current_day > 28 and current_month == 2 and current_year % 4 != 0)
        ):
            current_day = 1
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

    total_alerts = sum(alerts_by_day.values())
    logger.info(
        f"Prepared metadata for {len(metadata_records)} days (last {max_months_lookback} months: "
        f"{start_year}-{start_month:02d}-{start_day:02d} to {end_year}-{end_month:02d}-{end_day:02d}): "
        f"{total_alerts} total alerts distributed by day"
    )
    return metadata_records


def create_gfw_metadata_table(cursor, table_name):
    """Create the metadata table for GFW alerts."""
    cursor.execute(
        sql.SQL("""
        CREATE TABLE IF NOT EXISTS {metadata_table} (
            _id character varying(100) NOT NULL PRIMARY KEY,
            month smallint,
            year smallint,
            day smallint,
            total_alerts bigint,
            description_alerts text,
            type_alert text,
            data_source text
        );
    """).format(metadata_table=sql.Identifier(table_name))
    )
