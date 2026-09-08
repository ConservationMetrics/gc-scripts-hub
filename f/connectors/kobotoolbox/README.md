# KoboToolbox: Fetch Survey Responses

This script fetches form metadata and survey submissions from the KoboToolbox REST API.  Field translations are extracted from metadata and written to a PostgreSQL `labels` lookup table. The structured part of survey submissions are written to a PostgreSQL table, while media attachments are downloaded to disk in a specified directory. Form metadata is also saved to disk as a JSON file.

## Webhooks

The script can leverage [Windmill Webhooks](https://www.windmill.dev/docs/core_concepts/webhooks) to receive survey submissions from KoboToolbox, using KoboToolbox's [REST services](https://support.kobotoolbox.org/rest_services.html).

- In Windmill:
  - Open the KoboToolbox scriptand configure it with the parameters needed (KoboToolbox resource, form ID, etc.)
  - Click the **Triggers** tab, then **Webhooks**.
  - Generate a new webhook token and add it here.
  - Copy down the URL, body, and header token.
- Next, in KoboToolbox:
  - Open the survey you want to receive submissions from and click the **Settings** tab, then **REST Services**.
  - Add the webhook URL to the **Endpoint URL** field
  - Set the **Type** as JSON.
  - Add a **Custom HTTP Header** with the key `Authorization` and the value `Bearer <webhook_token>`.
  - In **Add custom wrapper around JSON submission**, add the content of the Windmill body.
  - Click the **Save** button.

New survey submissions will now be automatically sent by KoboToolbox to Windmill. You can validate that submissions were successfully sent in the **Rest Services** tab in KoboToolbox, and you should also see runs of the script in Windmill in the **Runs** tab.

> [!TIP]
>
> Using webhooks should not be a replacement for scheduling the script. You should still schedule the script to run on a regular interval to ensure that you are not missing any submissions in the event that either KoboToolbox or Windmill has an outage.

## Label Lookup Table (`__labels`)

The script creates a secondary table named `<table_name>__labels` to store question and choice labels from the form definition. If the form metadata includes translations (via the `translations` field), each translation is stored as a separate row—one per language—for each form element.

Each row represents one label for a form element (from either the `survey` or `choices` section), with the following structure: 

| Column          | Type    | Description                                                                 |
|-----------------|---------|-----------------------------------------------------------------------------|
| `type`          | TEXT    | Either `"survey"` or `"choices"` indicating the form section               |
| `question_name` | TEXT    | For choices: the survey question that uses this choice list. `NULL` for survey rows (and orphan choices with no referencing question). When multiple questions share a list, choice rows are duplicated once per question. |
| `name`          | TEXT    | The name of the form element (question or choice)                          |
| `language`      | TEXT    | The language of the label (e.g., `"en"`, `"es"`, `"pt"`)                    |
| `label`         | TEXT    | The label text in the specified language                                   |
| `_id`           | TEXT    | Deterministic hash based on the row content (used as a unique key)         |

This table can be used for rendering field translations in downstream clients, selecting the appropriate label by language, or falling back gracefully when a translation is missing. Choice value lookups should join on both `question_name` and `name` so reused raw values (e.g. shared numeric scales) resolve to the correct label.

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

> **Note:** The ODK connector (`f/connectors/odk/odk_responses.py`) shares this XLSForm-style plumbing but does not (yet) flatten nested payloads. See the `TODO` there.

## 📚 Reference

* KoboToolbox API Documentation: https://support.kobotoolbox.org/api.html