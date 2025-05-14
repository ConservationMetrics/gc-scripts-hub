# KoboToolbox: Fetch Survey Responses

This script fetches survey submissions from the KoboToolbox REST API, transforms them for SQL compatibility, and writes the results to a PostgreSQL database. It also downloads any media attachments and saves the form metadata to disk.

## Label Lookup Table (`__labels`)

The script creates a secondary table named `<table_name>__labels` to store question and choice labels from the form definition. If the form metadata includes translations (via the `translations` field), the table will include one column per language (e.g. `label_en`, `label_es`, etc.). If no translations are provided (e.g., `translations: [null]`), only the default label is included as a single `label` column.

Each row represents one form element (from either the `survey` or `choices` section), with the following structure:


| Column             | Description                                                           |
|--------------------|-----------------------------------------------------------------------|
| `type`             | Either `"survey"` or `"choices"` indicating the form section          |
| `name`             | The name of the form element (question or choice)                     |
| `label_<lang>`     | The label text in each language (e.g. `label_en`, `label_es`, etc.)   |
| `label`            | The default label (only present if no translations are defined)       |
| `_id`              | Deterministic hash based on the row content (used as a unique key)    |

This table can be used for rendering field translations on downstream front ends or clients.

## 📚 Reference

* KoboToolbox API Documentation: https://support.kobotoolbox.org/api.html