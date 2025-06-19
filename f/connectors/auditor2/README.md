# Auditor 2: Import Annotated Acoustic Data

This script processes annotated acoustic data from an [Auditor 2](https://github.com/ConservationMetrics/AuditorServer) project, via a ZIP file containing 5 expected CSVs and attachments (audio and spectrogram images).

The script expects the following five CSV files, each identified by specific substrings in their filenames:
- `deployments`: Must include a `deployment_id` field.
- `human_readable_labels`: No specific ID field required.
- `labels`: No specific ID field required.
- `sites`: Must include a `site_id` field.
- `sound_file_summary`: Must include a `deployment_id` field.

Currently, the script applies minimal transformations to the data. As with Timelapse, [we don't yet have clear requirements for the front end](https://github.com/ConservationMetrics/gc-scripts-hub/issues/102). However, unlike Timelapse, which uses a highly dynamic data schema, we can deterministically identify latitude and longitude fields in the Auditor 2 CSVs (specifically, the `sites` CSV). We therefore construct a `g__coordinates` column for usage with mapping front ends.

The script creates tables in the following structure:

```
auditor2_{project_name}_deployments
auditor2_{project_name}_human_readable_labels
auditor2_{project_name}_labels
auditor2_{project_name}_sites
auditor2_{project_name}_sound_file_summary
```

Additionally, the script will store files in the following directory schema:

```
{attachment_root}/auditor2/{project_name}/...
```

If the project name is already found in the database, the script will raise an error.

TODO: Figure out how the ZIP file makes it to blob storage, and what calls this script. c.f. https://github.com/ConservationMetrics/gc-scripts-hub/issues/106