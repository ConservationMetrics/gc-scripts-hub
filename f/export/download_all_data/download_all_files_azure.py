import logging
from datetime import datetime, timedelta

from azure.storage.blob import (
    BlobServiceClient,
    ContainerSasPermissions,
    generate_container_sas,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    blob_connection_string: str,
    container_name: str,
    folder_path: str = "",
    expiry_minutes: int = 120,
):
    """
    Generate a SAS URL for an Azure Blob Storage container or subfolder,
    allowing the user to run an `azcopy` command to download all files directly
    from persistent storage. Ideal for creating a fast and reliable file
    download option without requiring zipping large directories.

    Returns:
        dict: Dictionary containing the generated azcopy commands for different destinations
    """

    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            blob_connection_string
        )
        account_name = blob_service_client.account_name

        # Generate the SAS token
        sas_token = generate_container_sas(
            account_name=account_name,
            container_name=container_name,
            account_key=blob_service_client.credential.account_key,
            permission=ContainerSasPermissions(read=True, list=True),
            expiry=datetime.utcnow() + timedelta(minutes=expiry_minutes),
        )

        base_url = f"https://{account_name}.blob.core.windows.net/{container_name}"
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
