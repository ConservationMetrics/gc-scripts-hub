# KoboToolbox: Fetch Survey Responses

This script fetches survey submissions from the KoboToolbox REST API, transforms them for SQL compatibility, and writes the results to a PostgreSQL database. It also downloads any media attachments and saves the form metadata to disk.

In addition to storing raw submissions and metadata, the script builds a `<table_name>__labels` lookup table of form labels for multilingual support. If the form includes `translations` (as specified in the translations field of the metadata), the script extracts them into columns named `label_<code>`, where `<code>` corresponds to the language code, with one row per question or choice.

If no translations are provided (e.g., `translations: [null]`), a single `label` column is extracted instead with the default label.

For information on the KoboToolbox API, see [KoboToolbox API Documentation](https://support.kobotoolbox.org/api.html).