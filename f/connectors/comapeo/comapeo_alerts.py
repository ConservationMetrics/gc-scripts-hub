# requirements:
# psycopg2-binary
# requests~=2.32

import json
import logging
from datetime import datetime
from typing import TypedDict

import psycopg2
import requests

# type names that refer to Windmill Resources
postgresql = dict


class comapeo_server(TypedDict):
    server_url: str
    access_token: str


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conninfo(db: postgresql):
    """Convert a `postgresql` Windmill Resources to psycopg-style connection string"""
    # password is optional
    password_part = f" password={db['password']}" if "password" in db else ""
    conn = "dbname={dbname} user={user} host={host} port={port}".format(**db)
    return conn + password_part


def main(
    db: postgresql,
    comapeo: comapeo_server,
    comapeo_project: str,
    db_table_name: str = "alerts",
):
    comapeo_server_url = comapeo["server_url"]
    comapeo_alerts_endpoint = (
        f"{comapeo_server_url}/projects/{comapeo_project}/remoteDetectionAlerts"
    )

    comapeo_access_token = comapeo["access_token"]
    comapeo_headers = {
        "Authorization": f"Bearer {comapeo_access_token}",
        "Content-Type": "application/json",
    }

    alerts = get_alerts_from_db(conninfo(db), db_table_name)

    unposted_alerts = filter_alerts(comapeo_alerts_endpoint, comapeo_headers, alerts)

    if not unposted_alerts:
        logger.info("No new alerts to post!")
        return

    transformed_unposted_alerts = transform_alerts(unposted_alerts)

    post_alerts(comapeo_alerts_endpoint, comapeo_headers, transformed_unposted_alerts)


def get_alerts_from_db(db_connection_string, db_table_name: str):
    """
    Retrieves alerts from a PostgreSQL database table.

    Parameters
    ----------
    db_connection_string : str
        The connection string for the PostgreSQL database.
    db_table_name : str
        The name of the database table containing the alerts.

    Returns
    -------
    list
        A list of dictionaries, where each dictionary represents an alert row from the database table with keys for the column names.
    """
    logger.info("Fetching alerts from database...")

    conn = psycopg2.connect(dsn=db_connection_string)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {db_table_name}")
    alerts = [
        dict(zip([col.name for col in cur.description], row)) for row in cur.fetchall()
    ]
    cur.close()
    conn.close()

    logger.info(f"{len(alerts)} alerts found in database.")

    return alerts


def _get_alerts_from_comapeo(comapeo_alerts_endpoint: str, comapeo_headers: dict):
    """
    Fetches alerts from the CoMapeo API.

    Parameters
    ----------
    comapeo_alerts_endpoint : str
        The URL endpoint for retrieving alerts from the CoMapeo API.
    comapeo_headers : dict
        The headers to be included in the API request, such as authorization tokens.

    Returns
    -------
    set
        A set of alert source IDs for alerts that have been posted to the CoMapeo API.
    """
    logger.info("Fetching alerts from CoMapeo API...")
    response = requests.request(
        "GET", url=comapeo_alerts_endpoint, headers=comapeo_headers, data={}
    )

    response.raise_for_status()
    alerts = response.json().get("data", [])

    posted_alert_source_ids = {alert["sourceId"] for alert in alerts}

    logger.info(f"{len(posted_alert_source_ids)} alerts found on CoMapeo.")
    return posted_alert_source_ids


def filter_alerts(
    comapeo_alerts_endpoint: str, comapeo_headers: str, alerts: list[dict]
):
    """
    Filters a list of alerts to find those that have not been posted to the CoMapeo API.

    Parameters
    ----------
    comapeo_alerts_endpoint : str
        The URL endpoint for retrieving alerts from the CoMapeo API.
    comapeo_headers : str
        The headers to be included in the API request, such as authorization tokens.
    alerts : list[dict]
        A list of dictionaries, where each dictionary represents an alert.

    Returns
    -------
    list[dict]
        A list of dictionaries, where each dictionary represents an alert that has not been posted to the CoMapeo API.
    """
    logger.info("Filtering alerts...")

    alerts_posted_to_comapeo = _get_alerts_from_comapeo(
        comapeo_alerts_endpoint, comapeo_headers
    )

    # alert_id in the database matches sourceId on CoMapeo
    unposted_alerts = [
        alert
        for alert in alerts
        if alert.get("alert_id") not in alerts_posted_to_comapeo
    ]

    logger.info(f"{len(unposted_alerts)} alerts in database not yet posted to CoMapeo.")
    return unposted_alerts


def transform_alerts(alerts: list[dict]):
    """
    Transforms a list of alerts into a format that matches the expected schema on the CoMapeo API.

    Parameters
    ----------
    alerts : list[dict]
        A list of dictionaries, where each dictionary represents an alert.

    Returns
    -------
    list[dict]
        A list of dictionaries, where each dictionary represents an alert in a format that can be posted to the CoMapeo API.
    """
    logger.info("Transforming alerts...")

    transformed_alerts = [
        {
            # CoMapeo API requires these to be ISO 8601 format
            "detectionDateStart": datetime.strptime(
                alert["date_start_t0"], "%Y-%m-%d"
            ).isoformat(timespec="seconds")
            + "Z",
            "detectionDateEnd": datetime.strptime(
                alert["date_end_t0"], "%Y-%m-%d"
            ).isoformat(timespec="seconds")
            + "Z",
            "geometry": {
                "type": alert["g__type"],
                "coordinates": json.loads(alert["g__coordinates"]),
            },
            "metadata": {
                "alert_type": alert["alert_type"],
            },
            "sourceId": alert["alert_id"],
        }
        for alert in alerts
    ]

    return transformed_alerts


def post_alerts(
    comapeo_alerts_endpoint: str,
    comapeo_headers: dict,
    alerts: list[dict],
):
    """
    Posts a list of alerts to the CoMapeo API.

    Parameters
    ----------
    comapeo_alerts_endpoint : str
        The URL endpoint for posting alerts to the CoMapeo API.
    comapeo_headers : dict
        The headers to be included in the API request, such as authorization tokens.
    alerts : list[dict]
        A list of dictionaries, where each dictionary represents an alert to be posted to the CoMapeo API.
    """
    logger.info("Posting alerts to CoMapeo API...")

    for alert in alerts:
        response = requests.post(
            url=comapeo_alerts_endpoint, headers=comapeo_headers, json=alert
        )
        response.raise_for_status()

    logger.info(f"{len(alerts)} alerts posted successfully.")
