# KoboToolbox: Fetch Survey Responses

This script fetches form metadata and survey submissions from the KoboToolbox REST API.  Field translations are extracted from metadata and written to a PostgreSQL `labels` lookup table. The structured part of survey submissions are written to a PostgreSQL table, while media attachments are downloaded to disk in a specified directory. Form metadata is also saved to disk as a JSON file.

## Label Lookup Table (`__labels`)

The script creates a secondary table named `<table_name>__labels` to store question and choice labels from the form definition. If the form metadata includes translations (via the `translations` field), each translation is stored as a separate row—one per language—for each form element.

Each row represents one label for a form element (from either the `survey` or `choices` section), with the following structure: 

| Column     | Type    | Description                                                                 |
|------------|---------|-----------------------------------------------------------------------------|
| `type`     | TEXT    | Either `"survey"` or `"choices"` indicating the form section               |
| `name`     | TEXT    | The name of the form element (question or choice)                          |
| `language` | TEXT    | The language of the label (e.g., `"en"`, `"es"`, `"pt"`)                    |
| `label`    | TEXT    | The label text in the specified language                                   |
| `_id`      | TEXT    | Deterministic hash based on the row content (used as a unique key)         |

This table can be used for rendering field translations in downstream clients, selecting the appropriate label by language, or falling back gracefully when a translation is missing.

## 📚 Reference

* KoboToolbox API Documentation: https://support.kobotoolbox.org/api.html