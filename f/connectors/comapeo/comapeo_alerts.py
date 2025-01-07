# requirements:
# psycopg2-binary
# requests~=2.32

import json
import logging
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

    post_alerts(comapeo_alerts_endpoint, comapeo_headers, unposted_alerts)


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
        A list of tuples, where each tuple represents an alert row from the database table.
    """
    conn = psycopg2.connect(dsn=db_connection_string)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {db_table_name}")
    alerts = cur.fetchall()
    cur.close()
    conn.close()
    return alerts


def _get_alerts_from_comapeo(comapeo_alerts_endpoint: str, comapeo_headers: str):
    """
    Fetches alerts from the CoMapeo API.

    Parameters
    ----------
    comapeo_alerts_endpoint : str
        The URL endpoint for retrieving alerts from the CoMapeo API.
    comapeo_headers : str
        The headers to be included in the API request, such as authorization tokens.

    Returns
    -------
    list
        A list of dictionaries, where each dictionary represents an alert retrieved from the CoMapeo API.
    """
    logger.info("Fetching alerts from CoMapeo API...")
    response = requests.request(
        "GET", url=comapeo_alerts_endpoint, headers=comapeo_headers, data={}
    )

    response.raise_for_status()
    alerts = response.json().get("data", [])

    return alerts


def filter_alerts(comapeo_alerts_endpoint: str, comapeo_headers: str, alerts):
    """
    Filters a list of alerts to find those that have not been posted to the CoMapeo API.

    Parameters
    ----------
    comapeo_alerts_endpoint : str
        The URL endpoint for retrieving alerts from the CoMapeo API.
    comapeo_headers : str
        The headers to be included in the API request, such as authorization tokens.
    alerts : list
        A list of dictionaries, where each dictionary represents an alert.

    Returns
    -------
    list
        A list of dictionaries, where each dictionary represents an alert that has not been posted to the CoMapeo API.
    """
    alerts_posted_to_comapeo = _get_alerts_from_comapeo(
        comapeo_alerts_endpoint, comapeo_headers
    )

    # alert_id in the database matches sourceId on CoMapeo
    posted_source_ids = {alert["sourceId"] for alert in alerts_posted_to_comapeo}
    unposted_alerts = [
        alert for alert in alerts if alert["alert_id"] not in posted_source_ids
    ]

    return unposted_alerts


def post_alerts(
    comapeo_alerts_endpoint: str,
    comapeo_headers: str,
    unposted_alerts,
):
    """
    Posts a list of alerts to the CoMapeo API.

    Parameters
    ----------
    comapeo_alerts_endpoint : str
        The URL endpoint for posting alerts to the CoMapeo API.
    comapeo_headers : str
        The headers to be included in the API request, such as authorization tokens.
    unposted_alerts : list
        A list of dictionaries, where each dictionary represents an alert to be posted to the CoMapeo API.
    """
    logger.info("Posting alerts to CoMapeo API...")

    for alert in unposted_alerts:
        payload = json.dumps(alert)
        response = requests.request(
            "POST", url=comapeo_alerts_endpoint, headers=comapeo_headers, data=payload
        )
        response.raise_for_status()
        logger.info(f"Posted alert: {alert}")

    logger.info("All alerts posted successfully.")
