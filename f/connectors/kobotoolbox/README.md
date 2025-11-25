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

## Endpoints

The request header must include an API token in the format:

    Authorization: Token <api_key>

See the fixture data in the [server_responses.py](./tests/assets/server_responses.py) file for complete responses from the server for the endpoints below.

### `GET /api/v2/assets/:form_id/`

Retrieves metadata for a specific form, including the form name, owner, content structure (survey questions and choices), and translations.

```json
{
  "name": "Arboles",
  "uid": "mimsyweretheborogoves",
  "owner__username": "kobo_admin",
  "data": "http://kobotoolbox.example.org/api/v2/assets/mimsyweretheborogoves/data/",
  "content": {
    "schema": "1",
    "survey": [
      {
        "name": "Record_your_current_location",
        "type": "geopoint",
        "label": [
          "Record your current location",
          "Registre la ubicación actual",
          "Registre a localização atual"
        ],
        ...
      },
      ...
    ],
    "choices": [
      {
        "list_name": "tree_reasons",
        "name": "shade",
        "label": ["Shade", "Sombra", "Sombra"]
      },
      ...
    ],
    "settings": {
      "version": "1",
      "default_language": "English (en)"
    },
    "translations": ["English (en)", "Spanish (es)", "Portuguese (pt)"]
  }
}
```

### `GET /api/v2/assets/:form_id/data/`

Retrieves form submissions with pagination support. Query parameters:
- `limit`: Maximum number of results per page (default: 100, max: 1000 as of January 2026)
- `start`: Starting index for pagination (default: 0)

The response includes `next` and `previous` URLs for navigating through pages of results.

```json
{
  "count": 3,
  "next": "http://kobotoolbox.example.org/api/v2/assets/mimsyweretheborogoves/data/?limit=100&start=100",
  "previous": null,
  "results": [
    {
      "_id": 124961136,
      "formhub/uuid": "f7bef041e8624f09946bff05ee5cbd4b",
      "start": "2021-11-16T15:44:56.464-08:00",
      "end": "2021-11-16T15:46:17.524-08:00",
      "Record_your_current_location": "36.97012 -122.0109429 -14.432169171089349 4.969",
      "Estimate_height_of_your_tree_in_meters": "18",
      "Take_a_photo_of_this_tree": "1637106370994.jpg",
      "meta/instanceID": "uuid:e58da38d-3eee-4bd7-8512-4a97ea8fbb01",
      "_attachments": [
        {
          "download_url": "http://kobotoolbox.example.org/api/v2/assets/mimsyweretheborogoves/data/124961136/attachments/46493730/",
          "mimetype": "image/jpeg",
          "filename": "cmi_admin_kobo_test/attachments/.../1637106370994.jpg",
          ...
        }
      ],
      "_geolocation": [36.97012, -122.0109429],
      "_submission_time": "2021-11-16T23:46:27",
      ...
    },
    ...
  ]
}
```

### `GET /api/v2/assets/:form_id/data/:submission_id/attachments/:attachment_id/`

This endpoint retrieves the binary data of a specific attachment (such as a photo or audio file) associated with a form submission. The response contains the raw binary content of the file, which is saved to disk in the configured attachment directory.

## 📚 Reference

* KoboToolbox API Documentation: https://support.kobotoolbox.org/api.html