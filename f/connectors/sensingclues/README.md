# Sensing Clues

[**Sensing Clues**](https://sensingclues.org/) is a wildlife-monitoring platform. Rangers and community members record observations in the [Cluey Data Collector](https://sensingclues.org/portal) app; Focus stores those records (and CSV uploads) as observations tagged with an ontology of species, activities, and other concepts.

## `sensingclues_observations.py`

Fetches observations for one or more Focus groups via the [sensingcluespy](https://sensingcluespy.readthedocs.io/en/latest/) client. Saves raw JSON and CSV to the datalake and writes rows to PostgreSQL.

Each observation becomes one row. Ontology concepts collapse into `conceptLabels` / `conceptIds` lists. Form fields in `attributes` flatten into columns (core fields such as `fileName` and `tags` win on collision). Geometry comes from `Observation.where`.

### Credentials

Create a personal account with the Cluey Data Collector app (Android; see [the Sensing Clues portal](https://sensingclues.org/portal)). Sensing Clues also publishes a read-only demo account, `demo` / `demo`, which can reach the public demo groups `focus-project-3494596` (Cluey app) and `focus-project-1234` (CSV upload).

### Finding group names

Group names look like `focus-project-<id>`. The script checks requested names against the groups the account can see (`search/all/facets`) and raises if any are missing, listing the available names.

## Future work

* **Windmill resource type.** Username and password are plain script parameters for now. A custom `sensingclues` resource would let operators reuse credentials across runs without re-entering them. 
* **Tracks and layers.** The Focus API and sensingcluespy both expose them; this script is observations only.
* **Images.** Cluey observations reference photos as opaque IDs in an `images` attribute (e.g. `b9c9-b7be-9d51`). There is no documented endpoint to resolve those IDs to files, so attachments are not downloaded. The IDs are preserved in the table.

## 📚 Reference

* [Sensing Clues](https://sensingclues.org/)
* [sensingcluespy usage](https://sensingcluespy.readthedocs.io/en/latest/source/usage.html)
* [sensingcluespy tutorial](https://sensingcluespy.readthedocs.io/en/latest/notebooks/sensingclues_tutorial.html)
* [Cluey API documentation](https://sensingclues.freshdesk.com/support/solutions/articles/48001248536-cluey-api-documentation)
