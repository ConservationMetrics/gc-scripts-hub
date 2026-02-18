# requirements:
# requests~=2.32

import logging
from typing import TypedDict

import requests


class comapeo_server(TypedDict):
    server_url: str
    access_token: str


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(comapeo: comapeo_server):
    """Count and report the number of projects on a CoMapeo server.

    Parameters
    ----------
    comapeo : comapeo_server
        Dictionary containing 'server_url' and 'access_token' for the CoMapeo server.

    Returns
    -------
    dict
        A dictionary containing the project count metric.
    """
    server_url = comapeo["server_url"]
    access_token = comapeo["access_token"]

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    url = f"{server_url}/projects"
    logger.info("Fetching projects from CoMapeo API...")

    response = session.get(url)
    response.raise_for_status()

    projects = response.json().get("data", [])
    project_count = len(projects)

    logger.info(f"Total number of projects on CoMapeo server: {project_count}")

    return {"project_count": project_count}
