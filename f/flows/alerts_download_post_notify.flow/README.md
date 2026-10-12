# Alerts: Download, Post, and Notify

This flow connects three scripts:

1. [Alerts: Fetch alerts from Google Cloud Storage](../../connectors/alerts/README.md)
2. [CoMapeo: Post Alerts](../../connectors/comapeo/README.md#comapeo_alerts-post-alerts-to-comapeo-api)
3. [Twilio: Notify of change detection alerts](../../connectors/twilio/README.md)

The flow skips the CoMapeo and Twilio steps when their inputs are absent. The Twilio step is also skipped when the fetch step wrote no new alerts.

**Dataset** is `use_existing_dataset` or `create_new_dataset` (default). The form shows these as "Use existing dataset" and "Enter dataset name". Entering a name creates the dataset if needed and updates it if it already exists. CoMapeo and the Twilio link use that same table. A typed name is at most 53 characters.
