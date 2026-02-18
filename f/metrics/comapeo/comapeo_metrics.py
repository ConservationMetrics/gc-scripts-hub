# requirements:
# requests~=2.32

import logging
import subprocess
from pathlib import Path
from typing import Optional, TypedDict

import requests


class comapeo_server(TypedDict):
    server_url: str
    access_token: str


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_directory_size(directory_path: str) -> Optional[int]:
    """Get the size of a directory in bytes.

    Parameters
    ----------
    directory_path : str
        Path to the directory.

    Returns
    -------
    Optional[int]
        Size in bytes, or None if the path doesn't exist or access fails.
    """
    path = Path(directory_path)

    if not path.exists():
        logger.warning(f"Directory path does not exist: {directory_path}")
        return None

    try:
        result = subprocess.run(
            ["du", "-sb", str(path)],
            capture_output=True,
            text=True,
            check=True,
        )
        size_bytes = int(result.stdout.split()[0])
        return size_bytes
    except (subprocess.CalledProcessError, ValueError, IndexError, Exception) as e:
        logger.error(f"Failed to get directory size: {e}")
        return None


def main(
    comapeo: comapeo_server,
    attachment_root: str = "/persistent-storage/datalake",
):
    """Count projects and report data directory size for a CoMapeo server.

    Parameters
    ----------
    comapeo : comapeo_server
        Dictionary containing 'server_url' and 'access_token' for the CoMapeo server.
    attachment_root : str, optional
        Path to the datalake root directory where CoMapeo data is stored.
        Defaults to "/persistent-storage/datalake".

    Returns
    -------
    dict
        A dictionary containing metrics: project_count, data_size_mb.
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

    # Get size of comapeo data directory
    comapeo_data_path = Path(attachment_root) / "comapeo"
    data_size_bytes = get_directory_size(str(comapeo_data_path))

    metrics = {"project_count": project_count}

    if data_size_bytes is not None:
        data_size_mb = round(data_size_bytes / (1024**2), 2)
        metrics["data_size_mb"] = data_size_mb
        logger.info(
            f"CoMapeo data directory size (on datalake/comapeo dir, not the CoMapeo volume): {data_size_mb} MB"
        )
    else:
        logger.warning("Could not determine data directory size")

    return metrics
