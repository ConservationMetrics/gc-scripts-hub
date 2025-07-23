from unittest.mock import patch

from f.export.download_all_data.download_all_files_azure import main


@patch("f.export.download_all_data.download_all_files_azure.generate_container_sas")
def test_basic_output(mock_generate_sas):
    """Test that all expected azcopy commands are returned"""
    mock_generate_sas.return_value = "test_sas_token"

    # Create mock azure_blob resource
    azure_blob = {
        "accountName": "testaccount",
        "containerName": "testcontainer",
        "accessKey": "testkey",
        "useSSL": True,
        "endpoint": "core.windows.net",
    }

    commands = main(azure_blob)

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
def test_with_folder_path(mock_generate_sas):
    """Test that folder path is included in URLs"""
    mock_generate_sas.return_value = "test_sas_token"

    # Create mock azure_blob resource
    azure_blob = {
        "accountName": "testaccount",
        "containerName": "testcontainer",
        "accessKey": "testkey",
        "useSSL": True,
        "endpoint": "core.windows.net",
    }

    commands = main(azure_blob, "data/exports")

    # Check that folder path is included in URL
    expected_url = "https://testaccount.blob.core.windows.net/testcontainer/data/exports?test_sas_token"
    assert (
        commands["local"]
        == f"azcopy copy '{expected_url}' './downloaded_data/' --recursive"
    )
