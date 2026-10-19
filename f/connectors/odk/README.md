# ODK: Fetch Survey Responses

This script fetches form submissions from an ODK Central server using pyODK (an API client for the ODK Central API). It transforms the data for SQL compatibility, and stores it in a PostgreSQL database. Additionally, it downloads any attachments and saves them to a specified directory.

**Dataset** is `use_existing_dataset` or `create_new_dataset` (default). The form shows these as "Use existing dataset" and "Enter dataset name". Entering a name creates the dataset if needed and updates it if it already exists. Existing datasets are chosen from the public tables on `db`. A new dataset name is at most 54 characters and is also the datalake subdirectory.

For more information on the use of pyODK, see [KoboToolbox API Documentation](https://getodk.github.io/pyodk/).