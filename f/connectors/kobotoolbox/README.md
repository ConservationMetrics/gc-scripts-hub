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

## Nested Data Flattening (repeat groups & matrices)

KoboToolbox returns [repeat groups](https://support.kobotoolbox.org/group_repeat.html) and [matrix](https://support.kobotoolbox.org/matrix_response.html) questions as **lists of dicts** with long slash-separated keys. Left alone, each of these lands in a single cell that is difficult to analyze in downstream tools like Apache Superset. Before insertion, submissions are flattened into wide `{group}/{index}/{leaf}` keys (1-based index, `leaf` = last slash segment).

A repeat group from the API:

```json
"household_members": [
  { "household_members/member/member_name": "Ada",  "household_members/member/member_age": "40" },
  { "household_members/member/member_name": "Alan", "household_members/member/member_age": "37" }
]
```

is flattened to:

```
household_members/1/member_name = "Ada"
household_members/1/member_age  = "40"
household_members/2/member_name = "Alan"
household_members/2/member_age  = "37"
```

A field-list group arrives as a lone dict and is flattened with index `1`:

```json
"dwelling_counts": { "dwelling_counts/house/house_adults": "2", "dwelling_counts/house/house_children": "1" }
```
```
dwelling_counts/1/house_adults   = "2"
dwelling_counts/1/house_children = "1"
```

The existing `reverse_properties_separated_by="/"` logic then reverses each key into a SQL column, e.g. `household_members/1/member_name` → `member_name__1__household_members`.

> **Note:** The ODK connector (`f/connectors/odk/odk_responses.py`) shares this plumbing but does **not** yet flatten nested payloads. See the `TODO` there.

## 📚 Reference

* KoboToolbox API Documentation: https://support.kobotoolbox.org/api.html