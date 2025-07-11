from unittest.mock import Mock, patch

from f.export.download_all_data.download_all_files_azure import main


@patch("f.export.download_all_data.download_all_files_azure.generate_container_sas")
@patch("f.export.download_all_data.download_all_files_azure.BlobServiceClient")
def test_basic_output(mock_blob_service_client, mock_generate_sas):
    """Test that all expected azcopy commands are returned"""

    # Mock Azure SDK
    mock_client = Mock()
    mock_client.account_name = "testaccount"
    mock_client.credential.account_key = "testkey"
    mock_blob_service_client.from_connection_string.return_value = mock_client
    mock_generate_sas.return_value = "test_sas_token"

    commands = main("test_connection_string", "testcontainer")

    # Check that all destination options are present
    assert "local" in commands
    assert "s3" in commands
    assert "gcs" in commands
    assert "azure" in commands

    # Check that azcopy commands are formatted correctly
    expected_url = (
        "https://testaccount.blob.core.windows.net/testcontainer?test_sas_token"
    )
    assert (
        commands["local"]
        == f"azcopy copy '{expected_url}' './downloaded_data/' --recursive"
    )
    assert (
        commands["s3"]
        == f"azcopy copy '{expected_url}' 's3://your-bucket-name/downloaded_data/' --recursive"
    )
    assert (
        commands["gcs"]
        == f"azcopy copy '{expected_url}' 'gs://your-bucket-name/downloaded_data/' --recursive"
    )
    assert (
        commands["azure"]
        == f"azcopy copy '{expected_url}' 'https://youraccount.blob.core.windows.net/yourcontainer/downloaded_data/' --recursive"
    )


@patch("f.export.download_all_data.download_all_files_azure.generate_container_sas")
@patch("f.export.download_all_data.download_all_files_azure.BlobServiceClient")
def test_with_folder_path(mock_blob_service_client, mock_generate_sas):
    """Test that folder path is included in URLs"""

    # Mock Azure SDK
    mock_client = Mock()
    mock_client.account_name = "testaccount"
    mock_client.credential.account_key = "testkey"
    mock_blob_service_client.from_connection_string.return_value = mock_client
    mock_generate_sas.return_value = "test_sas_token"

    # Get returned commands with folder path
    commands = main("test_connection_string", "testcontainer", "data/exports")

    # Check that folder path is included in URL
    expected_url = "https://testaccount.blob.core.windows.net/testcontainer/data/exports?test_sas_token"
    assert (
        commands["local"]
        == f"azcopy copy '{expected_url}' './downloaded_data/' --recursive"
    )
