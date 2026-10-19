# Twilio

Scripts in this folder send WhatsApp messages through [Twilio content templates](https://www.twilio.com/docs/content) when new data has been written to the Guardian Connector data warehouse.

> [!NOTE]
>
> For both of these scripts, currently the Guardian Connector Explorer base URL is hardcoded to `https://explorer.<instance_slug>.guardianconnector.net`. For self-hosted instances, this will need to be updated so that the base URL itself is also configurable.

# `twilio_change_detection`: Notify of change detection alerts

This script sends a WhatsApp message to each recipient with a summary of the latest processed change detection alerts. The [Alerts: Download, Post, and Notify](../../flows/alerts_download_post_notify.flow/README.md) flow runs it after [`alerts_gcs`](../alerts/README.md) writes new alerts. The flow skips this step when no Twilio template or instance slug is set, or when that fetch wrote no new alerts.

The message body is a Twilio content template. Placeholders are filled from `alerts_statistics` and the Explorer link:

```
{{1}} new change detection alert(s) have been published on your alerts dashboard for the date of {{2}}. The following activities have been detected in your region: {{3}}. Visit your alerts dashboard here: {{4}}. If you are using CoMapeo with an archive server enabled, you can receive the alerts by synchronizing.
```

- `{{1}}` is `alerts_statistics.total_alerts`
- `{{2}}` is `alerts_statistics.date`
- `{{3}}` is `alerts_statistics.description_alerts`
- `{{4}}` is `https://explorer.<instance_slug>.guardianconnector.net/alerts/<db_table_name>`

Translations are available in English, Dutch, Portuguese, and Spanish. Each language has its own content template [SID](https://www.twilio.com/docs/glossary/what-is-a-sid). Set `content_sid` on the `twilio_message_template` resource to the SID for the language to send.

## Inputs

**Dataset** (`db_table_name`) is an existing public table on `db`. The picker omits connector sidecar tables (`__columns`, `__labels`, `__metadata`). The Explorer link points at that table.

**`instance_slug`** is the Guardian Connector instance slug used in the Explorer URL.

**`alerts_statistics`** is the object returned by the alerts fetch script: total alerts, date, and a description of detected activities.

**`twilio_message_template`** is a Windmill [`twilio_message_template`](https://hub.windmill.dev/resource_types/274/twilio_message_template) resource: account SID, auth token, origin number, recipient numbers, content template SID, and messaging service SID.

# `twilio_dataset_update`: Notify of new dataset records

This script sends a WhatsApp message when an upstream connector adds new rows to a dataset. It can be called by the CoMapeo observations connector or a GeoJSON boundary filter. Any spatial filtering happens upstream: the filter counts intersecting features and passes the count to this script, which formats and sends the message. No message is sent when the count is 0.

The Twilio content template body is `{{1}}`. The script fills `{{1}}` and leaves out empty clauses. `language` selects the fixed wording: English, Español, Português, Nederlands, Swahili, or Thai. `data_type`, `filter_column`, and `list_of_values` are included as given. When `data_type` is omitted, the script uses that language's word for "records".

Raw message body, no values filled in:

```
<count> new <data_type> were detected within an area of interest of <bbox> and written to *<table>*. <Filter_column>: <values>. View them here: <url>
```

English, without a boundary:

```
20 new CoMapeo observations were written to *comapeo_my_data*. Categories: animals, historical places, trees. View them here: https://explorer.springfield.guardianconnector.net/map/comapeo_my_data
```

Español, same CoMapeo inputs:

```
20 CoMapeo observations nuevos fueron escritos en *comapeo_my_data*. Categories: animals, historical places, trees. Consúltalos aquí: https://explorer.springfield.guardianconnector.net/map/comapeo_my_data
```

English, with a bounding box:

```
4 new features were detected within an area of interest of [[-61.8, 17.3], [-61.5, 17.6]] and written to *gov_parcels*. View them here: https://explorer.springfield.guardianconnector.net/map/gov_parcels
```

## Inputs

**`data_statistics`** is the write summary from the upstream script:

- `new_records` is required. A value of `0` skips the send.
- `data_type` defaults to the translated word for "records".
- `filter_column` and `list_of_values` add `<filter_column>: <values>`. The first word of `filter_column` is capitalized. If either is missing, that clause is left out. `list_of_values` may be a list or a string.

**Dataset** (`db_table_name`) is an existing public table on `db`. The picker omits connector sidecar tables (`__columns`, `__labels`, `__metadata`). The message and the map link both use that table.

**`bbox`** is optional, as `[[west, south], [east, north]]` or that value as JSON. When set, the first sentence uses the area-of-interest wording for `language`. When omitted, the sentence only says the records were written.

**`instance_slug`** is the Guardian Connector instance slug, such as `springfield`. The link is `https://explorer.<instance_slug>.guardianconnector.net/map/<db_table_name>`. When omitted, the message has no link.

**`language`** is `English`, `Español`, `Português`, `Nederlands`, `Swahili`, or `Thai`. The default is `English`.

**`twilio_message_template`** is the same resource type as `twilio_change_detection`; however, it should NOT use the same resource reference as that script, as the Template SID will be different.
