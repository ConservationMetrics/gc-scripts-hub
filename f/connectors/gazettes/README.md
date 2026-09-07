# Gazettes: Government site scraper to detections pipeline

This folder contains a configurable pipeline that scrapes a government website
for PDF documents, tracks which ones are new, downloads and caches them, turns
them to text (OCR-ing image-only PDFs with marker when pypdf finds no text
layer), parses spatial detections out of that text, filters detections to a
territory boundary, and writes them to a database table compatible with the
Guardian Connector explorer and WhatsApp alerting.

The stages are deliberately separate, reusable Windmill scripts so they can be
chained in different combinations (a `.flow` wires them together). The
intended end-to-end flow lives in `f/connectors/gazettes_ggmc.flow`.

## Scripts

### `gazettes_scrape`: Scrape a site for PDF links and track updates

- Fetches a listing page (e.g. <https://www.ggmc.gov.gy/gazettes/>) and selects
  document links with a configurable CSS selector (`pdf_selector`, default
  `#content a[href$='.pdf']`) and optional regex (`link_pattern`).
- Records every link in a state table `<db_table_name>__documents`. Only links
  never seen before are returned as `new_docs`, so a scheduled run processes a
  site's updates incrementally. Set `include_errored=True` for one run to retry
  documents previously marked `error` (e.g. after fixing the parser).
- Where to find parameters: the listing `site_url` from the government site; a
  `postgresql` resource; the `db_table_name` for your detections table.

### `gazettes_download`: Download PDFs into a local cache

- Downloads each new PDF into `cache_dir` under a URL-derived filename, so the
  same URL always maps to the same file and is never re-downloaded.
- When `check_updates` is on, cached files are HEAD-checked against their
  stored `ETag`/`Content-Length` to detect same-URL replacements (common on
  government sites). Downloads are streamed, verified to start with `%PDF`,
  and written atomically.
- Returns only genuinely new/changed PDFs so downstream stages skip the rest.

### `gazettes_pdf_to_text`: Convert cached PDFs to text files

- Extracts text with [pypdf](https://pypdf.readthedocs.io/) and caches it as a
  `.txt` file next to (or beside, per `text_dir`) each PDF; extraction is
  skipped when the text file is already newer than the PDF.
- **OCR fallback:** PDFs with no text layer (scans/photos of notices) or that
  fail to parse are OCR'd with [marker](https://github.com/datalab-to/marker),
  used **as a pip package in-process** (`ocr_fallback=True`, the default). The
  OCR'd markdown is cached as the same `.txt` file; entries report
  `"method": "pypdf" | "ocr" | "cached"`.
- **Marker worker prerequisites** (only needed when the OCR path is exercised):
  - `marker-pdf[full]` in the script lock — declare it as a Windmill *custom
    package* so workers don't re-resolve the torch stack per run;
  - model weights (~several GB) download on first use — set `MODEL_DIR` /
    `HF_HOME` to a `/persistent-storage` path on the worker so they survive;
  - a local inference backend: on CPU hosts the `llama-server` binary
    (llama.cpp) must be present on the worker image (GPU hosts: vLLM);
    marker spawns the server automatically.
  If any of these are missing, the document is recorded as a per-document
  error on the state table without stopping the rest of the batch.
- Set `ocr_fallback=False` to skip OCR entirely (image-only PDFs then simply
  error).

> [!WARNING]
> **Marker model weights:** the first OCR run downloads several GB of weights
> and starts a local inference server. Without a warm cache this can exceed
> Windmill's job timeout and re-downloads on every cold worker. Before
> enabling OCR in production, point `MODEL_DIR` and `HF_HOME` at a path under
> `/persistent-storage` on the worker (e.g. via the workspace env config), and
> keep the `marker-pdf[full]` lock declared as a custom package. On CPU-only
> workers the `llama-server` (llama.cpp) binary must also be installed in the
> worker image; if any prerequisite is missing, affected documents are
> recorded as errors — the rest of the batch still processes.

### `gazettes_ggmc_parse`: Parse GGMC gazette texts into GeoJSON

- Deterministic (regex) parser adapted from the GGMC Official Gazette spatial
  parser notebook: it segments a document into notice blocks, extracts
  file reference, dates, applicant/transferee, mining district, topographic
  map, acreage, statutory objection deadline, and the `Point A…` boundary
  vertices, converts DMS→decimal degrees (tolerating OCR/typist quirks), and
  builds shapely-validated polygons inside Guyana's bounds.
- Each feature gets a **deterministic id** (`uuid5(doc_url, file_reference,
  block_index)`) so re-runs upsert instead of duplicating.
- **Failure handling:** if any notice
  block fails to parse, the event is logged (`PARSE FAILURE <url>: ...` with
  the per-block `SchemaDriftError` reasons as the diagnosis), the document is
  marked `error`/`failed` on the state table with the failing blocks in
  `parse_notes`, and a `RuntimeError` is raised at the end of the run so the
  Windmill run fails visibly. Successfully parsed documents in the same run
  are still written to the data lake; extend the parser for the new shape,
  then rerun with `include_errored=true` to reprocess failed documents with
  zero re-download or re-extraction.

### `gazettes_filter_by_boundary`: Filter detections by boundary

- Keeps only features passing a spatial predicate (`intersects` by default, or
  `within`/`centroid`) against a boundary GeoJSON file in the data lake, so
  detections outside the area of interest never reach the database, explorer,
  or notifications.
- A boundary file can be a `Feature`, `FeatureCollection` (features are
  unioned), or a bare geometry. Features without geometry (non-spatial
  notices) are kept by default (`keep_null_geometry=False` to drop).

## State table (`<db_table_name>__documents`)

One row per scraped document URL: `url`, `link_text`, `status`
(`new`/`downloaded`/`text_ready`/`processed`/`error`), `local_path`, `etag`,
`content_length`, `parse_status`, `parse_notes` (JSON with the failing notice
blocks and their parser errors), `error`, `first_seen`, `last_seen`,
`last_processed`. This single table is what makes the whole pipeline
incremental and self-healing.

## Output database table

`gazettes_ggmc_parse` writes a merged FeatureCollection to `destination_path`;
`gazettes_filter_by_boundary` filters it; then the existing
`f/connectors/geojson/geojson_to_postgres` script loads it into
`<db_table_name>`. Columns follow the repo's flat `g__type`/`g__coordinates`
convention (see the `geojson` connector README), so `f/export/postgres_to_file`
can export it back to GeoJSON for explorer.

## Data lake layout

    /persistent-storage/datalake/gazettes/pdf/<basename>__<urlhash8>.pdf
    /persistent-storage/datalake/gazettes/pdf/<basename>__<urlhash8>.txt
    /persistent-storage/datalake/gazettes/<db_table_name>_<utc-timestamp>.geojson

## Usage (via the flow)

Run `f/connectors/gazettes_ggmc.flow` on a Windmill schedule. To reuse the
pipeline for a different government site, create a sibling flow that keeps
`gazettes_scrape → gazettes_download → gazettes_pdf_to_text → …
→ geojson_to_postgres → alerts_twilio` and swaps
`gazettes_ggmc_parse` for a site-specific parser that emits the same return
shape (`geojson_path` + `gazettes_statistics`).

## Future improvements

- **LLM-assisted reparse on parser failure.** The deterministic regex parser
  breaks when the gazette format drifts (reworded notices, new coordinate
  styles, table layouts). A previous iteration added an optional fallback that
  handled this well: when regex parsing failed for a document, its full text
  was sent to an OpenAI-compatible endpoint with a structured extraction
  prompt (segmentation rules, DMS sanitization, closed polygons, Guyana
  bounding-box guardrail); returned features were re-validated with the same
  shapely + bounds checks and merged without ever overriding regex-parsed
  results, and the model also diagnosed *why* the parser failed and proposed a
  fix. It was removed to keep the pipeline deterministic and alert-only — the
  current `PARSE FAILURE` logs plus `parse_notes` are what a human (or such a
  fallback, later) consumes. Re-adding it would mean: a `llm_fallback` flag
  on `gazettes_ggmc_parse`, an API key/base-url/model trio from Windmill
  variables, a validator mirroring `_validate_llm_geometry`, and id-space
  merging so a fallback feature only lands where the regex parser produced
  nothing.
