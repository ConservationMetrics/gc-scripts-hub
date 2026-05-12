# CyberTracker

[**CyberTracker**](https://www.cybertracker.org/) is a mobile-first data collection platform used for wildlife sightings, patrol observations, and other field monitoring. It comes in two flavors: the long-running **CyberTracker Classic** (a Windows desktop form designer paired with a fully-offline Android/iOS mobile app), and **CyberTracker Online** (a newer web-based form designer, data store, and reporting UI; the mobile app is still used for data capture).

CyberTracker Online allows you to download data as CSV, but it currently does not have an open API. Within the CyberTracker community, custom tooling has been built to get data out of CyberTracker like [this WordPress plugin](https://json4wp.dachspfad.de/) which uses [CyberTracker's webhook functionality](https://cybertrackerwiki.org/webhooks/).

On CyberTracker Mobile, it is possible to produce a **Backup** archive for both Classic and Online projects. Backup archives are ZIPs containing the form schema, observation records, and any attached photos / audio / tracks. The flow described below ingests backup data into Guardian Connector regardless of what flavor of CyberTracker project it is.

To create a backup from the mobile app, open the menu and go to **Settings → Data archive → Backup**. You will then be prompted to export a ZIP file. You can save the file to disk, or share via a communications tool like WhatsApp, Signal, or email.

> [!TIP]
> Once you have the file, you can use the `cybertracker_observations_from_backup.py` script, or the [GC Dataset Importer](https://docs.guardianconnector.net/reference/gc-toolkit/gc-scripts-hub/dataset-uploader) to import the data into Guardian Connector.


## Backup archive contents


The backup ZIP is structured as:

```text
archive/         # QML files describing form structure (one per schemaHash)
attachments/     # photos, audio recordings, KMZ tracks, GPS DBs
data/0.json      # the actual observation records
project.json     # device metadata and basic stats
```

The contents of `data/0.json` is an array of session/observation records keyed by `schemaHash` referencing the matching `.qml` in `archive/`. There can be records from multiple projects, including from both CyberTracker Online and CyberTracker Classic.

In `data/0.json`:

- **`cto_` in `fieldValues` keys** means the field is part of the **CyberTracker Online** capture envelope (device id, session start/end, location snapshot, username, etc.). This connector maps those keys to GeoJSON properties with a leading underscore (e.g. `cto_location` → `_location`).
- **CyberTracker Classic** data in the same backup format often uses unprefixed keys for similar ideas — for example a GPS ping may appear as `location` rather than `cto_location`.

## `cybertracker_observations_from_backup.py`

This script imports observations from a CyberTracker backup.

The script parses `data/0.json` and writes the observations to a PostgreSQL database and as a GeoJSON FeatureCollection file. The raw JSON file is also saved to the project folder for reference and can be used to re-import the data later.

> [!IMPORTANT]
> **Tracks vs observations.** Some sessions are **track-only** captures: `trackOnly: true` and/or `fieldValues.trackFile` pointing at a `.kmz` (plus companion `.db` under `attachments/`). Those rows have **no** point coordinates in `fieldValues` in the form this connector uses for a Point (`cto_location` / `_location` from **Online**, or plain `location` from **Classic**, each with numeric `x` / `y`).
> 
> These are **not** written to the observations GeoJSON: track geometry import is out of scope for now. To keep KMZ / DB / other track files, upload them with [Filebrowser](https://docs.guardianconnector.net/reference/gc-toolkit/filebrowser/).

## 📚 Reference

* CyberTracker website: <https://www.cybertracker.org/>
* CyberTracker Classic wiki: <https://cybertrackerwiki.org/classic/>
* CyberTracker Online wiki: <https://cybertrackerwiki.org/online/>