# Auditor 2: Import Annotated Acoustic Data

This script processes annotated acoustic data from an [Auditor 2](https://github.com/ConservationMetrics/AuditorServer) project. It downloads a ZIP file from Azure Blob Storage containing 5 expected CSVs and attachments (audio and spectrogram images).

The script requires Azure Blob Storage credentials to download the Auditor 2 ZIP file, which should be uploaded to a blob container before running the script.

The script expects the following five CSV files, each identified by specific substrings in their filenames:
- `deployments.csv`: Must include a `deployment_id` field.
- `human_readable_labels.csv`: No specific ID field required.
- `labels.csv`: No specific ID field required.
- `sites.csv`: Must include a `site_id` field.
- `sound_file_summary.csv`: Must include a `deployment_id` field.

Currently, the script applies minimal transformations to the data. As with Timelapse, [we don't yet have clear requirements for the front end](https://github.com/ConservationMetrics/gc-scripts-hub/issues/102). However, unlike Timelapse, which uses a highly dynamic data schema, we can deterministically identify latitude and longitude fields in the Auditor 2 CSVs (specifically, the `sites` CSV). We therefore construct a `g__coordinates` column for usage with mapping front ends.

The script creates tables using the following structure:

```
auditor2_{project_name}_deployments
auditor2_{project_name}_human_readable_labels
auditor2_{project_name}_labels
auditor2_{project_name}_sites
auditor2_{project_name}_sound_file_summary
```

It also stores files using the following directory schema:

```
{attachment_root}/auditor2/{project_name}/...
```

The script enforces uniqueness for the `project_name`. If any of the corresponding database tables already exist, the script will raise an error.