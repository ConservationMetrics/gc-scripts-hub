# Timelapse: Import Annotated Camera Trap Data

This script ingests annotated camera trap data from a [Timelapse](https://timelapse.ucalgary.ca/) project. It expects a ZIP archive of the entire project folder, and extracts tabular data from the `TimelapseData.ddb` (SQLite) database file. Media files such as images or videos are saved to the configured data lake.

The script writes data to PostgreSQL and handles fully dynamic schemas, since Timelapse allows all field names and folder levels to be customized by the user.

TODO: Figure out how the ZIP file makes it to blob storage, and what calls this script. c.f. https://github.com/ConservationMetrics/gc-scripts-hub/issues/106

> [!NOTE]
> The Timelapse database schema may change in future versions. This script was developed against Timelapse version 2.3.3.0.

## Notes

The script reads the `TimelapseData.ddb` Timelapse database and ingests the `DataTable` (containing annotations) and folder-level metadata tables (`Level1`, `Level2`, etc.). These tables represent user-defined folder hierarchies such as **project → station → deployment** and are described in the `FolderDataInfo` table, which maps level numbers to user-facing names. There can be any number of folder levels (`n`), but in practice only a few (typically 2–3) are commonly used.

Because both field names and folder levels can be customized in Timelapse, this script does not assume any fixed schema. Instead, it introspects the database at runtime to construct tables accordingly.

### Why not use Timelapse CSV export?

Although Timelapse offers a one-click CSV export option, the `.ddb` database includes richer metadata — including user-defined tooltips, field properties, and folder-level metadata. This enables more complete ingestion for long-term archival, analysis, and future reconstruction of the Timelapse project if needed.

Additionally, filenames and field order in the CSV can vary based on user-defined folder levels and field configurations. This makes the CSV format inconsistent across projects. In contrast, the `.ddb` provides a stable and comprehensive source of truth.

Users must access the project root folder anyway to retrieve media files, so zipping and uploading the full folder (including the `.ddb`) adds no additional burden.

### What about the `Backups/` subdirectory and other files?

The `Backups/` subdirectory, which is sometimes automatically created by Timelapse, is ignored by this script. To speed up upload time, you may also opt to exclude this subdirectory from your ZIP archive to begin with.

Other files not required for ETL, such as the `TimelapseTemplate.tdb` database, will still be copied over in case they are useful for reconstructing the Timelapse project in the future.

## Example Folder Structure

Below is a sample Timelapse project directory structure:

```
├── Station1
│   ├── Deployment1a
│   │   ├── IMG_001.jpg
│   │   └── IMG_002.jpg
│   └── Deployment1b
│       └── IMG_001.jpg
├── Station2
│   └── Deployment2a
│       └── IMG_001.jpg
└── TimelapseData.ddb
└── TimelapseTemplate.tdb
└── Backups (NOT COPIED BY THIS SCRIPT)
```

AS mentioned above, this folder structure is **user-defined** and will vary depending on how folder levels (e.g., station, deployment) are configured within Timelapse. The actual folder schema and names are defined by the user via the Timelapse UI; the application then stores these in the `.ddb` database.


## 📚 Reference
Timelapse user guides and documentation are frequently updated. For the most current details, consult: https://timelapse.ucalgary.ca/guides/




