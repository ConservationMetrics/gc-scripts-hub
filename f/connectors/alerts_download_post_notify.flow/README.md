# Alerts: Download, Post, and Notify

This flow connects three scripts: (1) Alerts: Fetch alerts from Google Cloud
Storage (2) CoMapeo: Post Alerts (3) Alerts: Send a Twilio Message.

The flow is designed to skip the CoMapeo and Twilio scripts if the right inputs
are not provided. Additionally, the Twilio script will be skipped if no new
alerts were written to the database during the Fetch alerts script.

**Dataset** is `use_existing_dataset` or `create_new_dataset` (default). Existing datasets are chosen from the public tables on `db`. CoMapeo and the Twilio link use that same table. A new name is at most 53 characters.