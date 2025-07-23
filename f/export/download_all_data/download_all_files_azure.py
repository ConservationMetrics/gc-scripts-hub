import logging
from datetime import datetime, timedelta

from azure.storage.blob import (
    ContainerSasPermissions,
    generate_container_sas,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# type names that refer to Windmill Resources
azure_blob = dict


def main(
    azure_blob: azure_blob,
    folder_path: str = "",
    expiry_minutes: int = 120,
):
    """
    Generate a SAS URL for an Azure Blob Storage container or subfolder and provide
    azcopy commands for transferring files to multiple destinations
    (local disk, AWS S3, Google Cloud Storage, and another Azure Storage account).

    Parameters
    ----------
    azure_blob : azure_blob
        Windmill Azure Blob Storage resource containing accountName, containerName,
        accessKey, useSSL, and optional endpoint.
    folder_path : str, optional
        The path to the subfolder within the container.
    expiry_minutes : int, optional
        The number of minutes before the SAS URL expires (default: 120)

    Returns
    -------
    dict
        Dictionary with keys 'local', 's3', 'gcs', 'azure' containing
        the corresponding azcopy commands for each destination
    """

    try:
        # Extract values from Windmill resource
        account_name = azure_blob["accountName"]
        container_name = azure_blob["containerName"]
        access_key = azure_blob["accessKey"]
        use_ssl = azure_blob.get("useSSL", True)
        endpoint = azure_blob.get("endpoint", "core.windows.net")

        # Generate the SAS token
        sas_token = generate_container_sas(
            account_name=account_name,
            container_name=container_name,
            account_key=access_key,
            permission=ContainerSasPermissions(read=True, list=True),
            expiry=datetime.utcnow() + timedelta(minutes=expiry_minutes),
        )

        protocol = "https" if use_ssl else "http"
        base_url = f"{protocol}://{account_name}.blob.{endpoint}/{container_name}"
        if folder_path:
            folder_path = folder_path.strip("/")
            full_url = f"{base_url}/{folder_path}?{sas_token}"
        else:
            full_url = f"{base_url}?{sas_token}"

        # Create command dictionary
        commands = {
            "local": f"azcopy copy '{full_url}' './downloaded_data/' --recursive",
            "s3": f"azcopy copy '{full_url}' 's3://your-bucket-name/downloaded_data/' --recursive",
            "gcs": f"azcopy copy '{full_url}' 'gs://your-bucket-name/downloaded_data/' --recursive",
            "azure": f"azcopy copy '{full_url}' 'https://youraccount.blob.core.windows.net/yourcontainer/downloaded_data/' --recursive",
        }

        # Print multiple destination options
        # This is a temporary solution to return the commands to the user
        # In the future, we might want to provide the commands in a Windmill app or something else
        print(
            "✅ Choose your preferred download destination and run the corresponding command from your terminal:"
        )
        print(" ")

        print("📁 **Download to local disk:**")
        print(commands["local"])
        print(" ")
        print("☁️ **Copy to AWS S3 bucket:**")
        print(commands["s3"])
        print("   (Requires AWS credentials configured: aws configure)")
        print(" ")
        print("🌐 **Copy to Google Cloud Storage:**")
        print(commands["gcs"])
        print("   (Requires Google Cloud credentials: gcloud auth login)")
        print(" ")
        print("💾 **Copy to another Azure Storage account:**")
        print(commands["azure"])
        print("   (Requires destination storage account credentials)")
        print(" ")
        print(f"⏰ **Note:** This SAS URL will expire in {expiry_minutes} minutes")
        print(
            "📖 **Documentation and installation instructions:** https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10"
        )

        return commands

    except Exception as e:
        logger.error("Failed to generate SAS URL or azcopy command.")
        raise e
