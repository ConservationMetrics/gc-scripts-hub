# ODK: Fetch Survey Responses

This script fetches form submissions from an ODK Central server using pyODK (an API client for the ODK Central API). It transforms the data for SQL compatibility, and stores it in a PostgreSQL database. Additionally, it downloads any attachments and saves them to a specified directory.

For more information on the use of pyODK, see [KoboToolbox API Documentation](https://getodk.github.io/pyodk/).