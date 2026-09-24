---
slug: importer-v2
created_at: September 14, 2026 6:20 PM
edited_at: September 16, 2026 4:01 PM
semver: 2.0.0
---

# Dataset Importer V2
 
This is the original feature spec written before implementation, it defines all the key acceptance criteria for feature completeness. It is preserved here for record-keeping and to document our original intent. If we change any core requirements in the importer app we should update this spec as source of truth.

# Background

A Dataset Importer app was already created as a multi-step Windmill App. It was used to upload .geojson, csv, Shapefiles and other data formats. Users reported frustration with [‘silent duplicates’](https://github.com/ConservationMetrics/gc-scripts-hub/issues/196) stemming from the inability to deduplicate datasets that lack unique identifiers. This spec was written to describe a revision to that data upload journey and close the issue linked above. In the new version described below, we gather user intent and enable the user to define ‘row identity’ by selecting candidate id column(s). We also made minor UX refinements and improved ZIP handling.

## Jobs to be Done

- **I have an old `.geojson` file of bird observations, and I want to view them on a satellite basemap so I can see if any sightings overlap with my tribal territory**
    
    The file contains 1,000 features which are species observations captured with the Mapeo app as geojson. I need a way to upload that data so I can view it in Explorer, as-is.
    
- **I want to understand which observations changed since my last Kobo field survey.**
    
    I exported the same Kobo project twice. The second export contains mostly the same observations, a few new sightings, and edits to some notes and locations. I need to compare the exports without counting unchanged observations as new sightings. The features do not have stable identifiers.
    
- **I want to create and upload dummy data during a CoMapeo training workshop, and I want that dummy data to be deleted when I re-upload the real dataset at a later date**
    
    From Rudo:
    
    > One of our users just brought this up on a call. Here is a user story. CoMapeo projects often start with a training workshop, where a bunch of dummy / training data is generated. Most of the time, this is unwanted data that ends up being deleted in the app. If Guardian Connector has already received and written this training data before the data was deleted, then the next time the script runs, it could detect the change upstream and remove the records from the dataset.
    [https://github.com/ConservationMetrics/gc-scripts-hub/issues/288#issuecomment-5681792735](https://github.com/ConservationMetrics/gc-scripts-hub/issues/288#issuecomment-5681792735)
    > 
- **I’m editing data manually externally in QGIS, importing to the data warehouse, for long term storage and to visualize in Explorer as a map view**
    
    

# Feature Components

## Core

The feature is comprised of a multi-step upload journey. The user navigates between steps with “Next” and “Back buttons”. The Next button is disabled until valid input is provided. Some steps are required, and some steps are conditional.

1. What’s your goal? (required)
    1. **Create** a new dataset
    2. **Append** to an existing dataset. Do not match, update, nor remove any existing records.
    3. **Sync** an existing dataset. Match and update records, add new records, and **remove** **existing records** not present in the import.
    4. **Merge** into an existing dataset. Match and update records, add new records, and **keep** **existing** **records** not present in the import.
2. Name your new dataset (conditional on `1A`)
3. Select target dataset (conditional on `1B OR 1C OR 1D`)
4. Upload your data (required)
5. Choose record identity (conditional on `1C or 1D`)
    1. Select one or more uniqueness column(s), in order.
    2. Choose update policy for records that exist in both datasets.
        1. Imported records win (default)
        2. Existing records win
6. Review & confirm changes
    1. Accept
    2. Go back

```yaml
CORE:
  1: Next button is disabled until valid input is provided
  2: Back button is visible, and if I navigate backwards my prior inputs are preserved
  3: Step counters or journey progress is visible, and reflects the initial goal selection.
```

**Design Hints**

Visual graphics can communicate the difference between each option in a clearer way. Bold text should highlight the keywords.

![image.png](Dataset%20Importer/image.png)

## Name your dataset

The ‘name your dataset’ step invites the user to name their new dataset (if they are creating a new dataset)

```yaml
NAME:
  1: The 'Name your new dataset' step only appears if I select Create (1A)
  2: Dataset name must be unique, otherwise helpful error is shown
  2-1: User input triggers a debounced uniqueness query and a pending state
```

## Select Target Dataset

The ‘select target dataset’ step allows the user to pick from compatible existing datasets.

```yaml
TARGET:
  1: A dropdown select is displayed and renders the name of every compatible existing dataset
  2: Datasets are rendered in alphabetical order
  3: By default, no dataset is selected
  4: If no compatible datasets are available, I see a helpful message
```

## Upload

The upload component is the step where a user drags or selects a file to upload.

Notes, we inherit some behavior from before; these are not strict requirements and can be refined at a layer date.

```yaml
UPLOAD:
  1: I can "drop to upload" with a visible target drop area and hint text
  2: I can "click to upload" which triggers the OS file browser on my device
  2-1: My OS file browser represents accepted file types (e.g. via the html `accept` property)
  3: Accepted file types are CSV, GeoJSON, GPX, GeoPackage, JSON, KML, Shapefile (.zip), XLS, XLSX, and SMART XML
  4: If I drag or upload an invalid file type, I see a helpful error
  5: A geopackage must contain exactly one spatial layer
  6: JSON expects a non-empty top level array of objects
  7: CyberTracker backup JSON is detected by content and converted automatically
  8: SMART patrol XML is detected by namespace and converted automatically
```

KoboToolbox and ODK source-specific transformations remain the responsibility
of their dedicated connectors. The importer does not ask the user to select a
data source and does not infer KoboToolbox or ODK from generic tabular exports.

## ZIP Uploads

The ZIP component specifies how exactly we should support zip file uploads and handle related edge cases. A zip can contain a shapefile fileset (`.shp` + `.shx` + `.dbf`), or a single file, or multiple files. The policy is to accept-all or reject-all because we don’t want users to be surprised by a partial upload. Schema does not need to match between files.

Special case: uploading multiple zipped files to a new data set is always treated as append-only. If the user would prefer to merge/deduplicate, they can perform this by uploading in multiple steps (first by selecting ‘Create new dataset’ then later selecting ‘Merge data with existing data set’ and choosing a merge strategy). This is the encapsulated as requirement `importer.ZIP.4` below. The reason for this append-only requirement is that supporting merge-and-create would complicate the core user journey described at the top of this doc.

```yaml
ZIP:
  1: If I upload a zip containing `.shp`, it is assumed to be a single shapefile layer; unrelated files lead to rejection.
  2: If my zip contains any unsupported file, the entire upload is rejected.
  3: If I upload a zip containing 1 or more supported files, they are merged or appended in order of appearance.
  4: If I upload a zip containing multiple files to a new dataset, I am notified that rows are append-only and will not be deduplicated or merged.
```

## Record identity

This step is required for users who selected Sync or Merge. The user must manually select one or more fields to define row identity. Preselecting candidate identifiers is out of scope for now. Validation of selected identifiers is also out of scope for now.

The user is also invited to select an “Update Policy” (either 'Imported records win' or 'Existing records win')

```yaml
IDENTITY:
  1: I can select from a list of candidate fields to be used as row identifiers
  2: The same field may not be selected twice
  3: I can select a maximum of 3 fields
  4: Record identity selections refer to dataset fields through the column-name map, so matching is not affected by source-column sanitization.
  5: For geojson, top level `feature.id` is included, alongside fields in `feature.properties`
  
POLICY:
	1: I must select from two options; 'Imported records win' or 'Existing records win'
	2: The default selection is 'Imported records win'
	3: User education / help text explains the difference between the two
```

**Design Hints**

Consider badges with the ability to add and remove selections. Candidates can be listed in a dropdown or popover

![image.png](Dataset%20Importer/image%201.png)

## Review changes

The final step calculates a preview of what will change if the upload is accepted. In this phase we also do preliminary validation that the chosen identity columns and uploaded data are valid.

```yaml
VALIDATION:
  1: If I try to 'Sync' or 'Create' with an empty dataset import, an error is thrown.
  2: If I try to 'Append' or 'Merge' with an empty dataset import, it's a noop (no changes).
  3: If my chosen identity columns contain duplicates (in input dataset or in source dataset), I am shown a helpful error.

```

```yaml
REVIEW:
	1: I am shown a preview of changes and asked to confirm.
  1-1: Number of records to be deleted
  1-2: Number of records to be updated
  1-3: Number of records to be added
  1-4: Number of columns to be added
  1-5: Number of unchanged records
  1-6: Final record count after import
  2: If the import is valid, I can click 'confirm'
  3: If after the final confirmation, an import error is encountered, it is surfaced in the UI
  4: After input, I am invited to 'Import another dataset' which clears state and brings me back to step 1
  5: If the dataset or my preview changes before I confirm, the import is stopped and I must review it again
  6: Confirming the same import more than once does not import the data twice
```

```yaml
# codify how merge/update/append goals are treated for rows with matching identity
MATCH:
  1: | 
    If my goal is Append, identity is ignored
    I may insert duplicate records, even if they have an id column.
  2: | 
    If my goal is Sync, matches are resolved based on my Update Policy [importer.POLICY.1]
    Existing rows without a match are removed.
  3: |
    If my goal is Merge, matches are resolved based on my Update Policy [importer.POLICY.1]
    Existing rows without a match are retained.
  4: Columns are never removed, even if they are omitted in the imported dataset.
  4-1: An omitted column in an imported dataset is not considered a deletion; existing fields remain.
  5: A field with a null/empty value clears any corresponding existing value, following chosen Update Policy
  5-1: 'Imported records' win means matching rows update with every imported field, including null/empty values; fields omitted from the import remain unchanged.
  5-2: 'Existing records' win means matching rows remain entirely unchanged.
```

**Design Hints**

If a card contains 0, it should be a white background with gray icon. If a card contains a value greater than zero, it should contain a colored background to draw the users attention to the actual changes (red for delete, blue for update etc.)

![image.png](Dataset%20Importer/image%202.png)

# Constraints & Engineering

## Limits

```yaml
LIMITS:
  1: Max 25 MiB source upload, to prevent excessive memory consumption
  2: Max 100 MiB uncompressed for ZIP and XLSX files, to avoid excessive memory use
  3: Max 150 columns per dataset
  4: A ZIP can contain at most 1,000 files
```

## Data

```yaml
  DATA:
    1: After a successful import, the original file is saved in the existing 'datalake'
    1-1: Unfinished uploads are kept temporarily for review or retry, then cleaned up after 24 hours when the importer runs again
    1-2: While a new dataset is staged, another import cannot create a dataset with the same name
    2: Perform import validation, change counting, and record matching in PostgreSQL using staging tables and SQL operations; do not load either the imported dataset or target dataset into application memory for comparison.
    3: Spatial imports preserve geometry type and spatial position
    3-1: If a tabular file has longitude and latitude columns, use them to place records on the map; warn me when coordinates are incomplete or invalid
    3-2: The preview shows how many imported records have valid map geometry
    3-3: GeoJSON GeometryCollection is not supported; show a helpful error instead of importing it
    4: Source column names are converted to stable, SQL-safe stored column names per established convention.
    4-1: If source column mapping does not exist for a dataset, it is created
    4-2: If two source column names result in identical strings after sanitization, the import is rejected and the user is asked to rename a column.
    4-3: If a dataset already has a column-name map, imports reuse it so the same fields stay connected
    5: Nested or multi-value source fields are retained as structured text
    6: Imports are all-or-nothing, if it fails, no data is written to the target dataset
```

# Out of scope / won’t ship

No “replace dataset” function. The user must manually create a new dataset, delete the old one, and create new views in gc-explorer or elsewhere.

No indexes on user-generated columns, this would improve merge/update performance but adds complexity and there are some minor unanswered questions about how to maintain them if the user changes row identity later on.

Won’t pre-validate identity columns during selection, that happens later during the Review Changes step and at import time.

Won’t pre-select candidate identifier columns. Just unneeded complexity for now. User must pick.
