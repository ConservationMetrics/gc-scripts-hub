# requirements:
# psycopg2-binary
# requests~=2.32

import json
import logging
import re
from pathlib import Path

import requests

from f.common_logic.db_operations import postgresql
from f.common_logic.save_disk import save_data_to_file
from f.connectors.geojson.geojson_to_postgres import main as save_geojson_to_postgres

# type names that refer to Windmill Resources
c_gfw_api = dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    gfw_api: c_gfw_api,
    bounding_box: str,
    type_of_alert: str,
    minimum_date: str,
    db: postgresql,
    db_table_name: str,
    attachment_root: str = "/persistent-storage/datalake",
):
    storage_path = Path(attachment_root) / db_table_name

    alerts = fetch_alerts_from_gfw(
        gfw_api["api_key"], bounding_box, type_of_alert, minimum_date
    )

    alerts_geojson = format_alerts_as_geojson(alerts, type_of_alert)

    save_data_to_file(
        alerts_geojson,
        db_table_name,
        storage_path,
        file_type="geojson",
    )

    rel_geojson_path = Path(db_table_name) / f"{db_table_name}.geojson"

    save_geojson_to_postgres(
        db,
        db_table_name,
        rel_geojson_path,
        attachment_root,
        False,  # to not delete the GeoJSON file after its contents are written to the database.
    )
    logger.info(f"GeoJSON data saved to PostgreSQL table [{db_table_name}].")


def fetch_alerts_from_gfw(
    api_key: c_gfw_api, bounding_box: str, type_of_alert: str, minimum_date: str
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
