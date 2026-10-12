# Twilio

Scripts in this folder send WhatsApp messages through [Twilio content templates](https://www.twilio.com/docs/content) when new data has been written to the Guardian Connector data warehouse.

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
