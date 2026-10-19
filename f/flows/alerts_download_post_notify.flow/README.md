# Alerts: Download, Post, and Notify

This flow connects three scripts:

1. [Alerts: Fetch alerts from Google Cloud Storage](../../connectors/alerts/README.md)
2. [CoMapeo: Post Alerts](../../connectors/comapeo/README.md#comapeo_alerts-post-alerts-to-comapeo-api)
3. [Twilio: Notify of change detection alerts](../../connectors/twilio/README.md)

The flow skips the CoMapeo and Twilio steps when their inputs are absent. The Twilio step is also skipped when the fetch step wrote no new alerts.
