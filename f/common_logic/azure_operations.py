# requirements:
# azure-storage-blob

import logging
import tempfile
from pathlib import Path

from azure.storage.blob import BlobServiceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_blob_to_temp(
    connection_string: str, container_name: str, blob_name: str
) -> Path:
    """
    Downloads a blob from Azure Blob Storage to a temporary file.

    Parameters
    ----------
    connection_string : str
        Azure Storage connection string
    container_name : str
        Name of the container
    blob_name : str
        Name of the blob to download

    Returns
    -------
    Path
        Path to the temporary file containing the downloaded blob
    """
    try:
        # Create BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )

        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )

        # Create temporary file with the same name as the blob
        temp_dir = Path(tempfile.gettempdir())
        temp_path = temp_dir / blob_name

        # Download blob to temporary file
        with open(temp_path, "wb") as download_file:
            blob_data = blob_client.download_blob()
            blob_data.readinto(download_file)

        logger.info(
            f"Downloaded blob '{blob_name}' from container '{container_name}' to {temp_path}"
        )
        return temp_path

    except Exception as e:
        logger.error(
            f"Failed to download blob '{blob_name}' from container '{container_name}': {e}"
        )
        raise
