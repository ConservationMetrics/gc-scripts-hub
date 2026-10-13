# Gazettes: Government site scraper to detections pipeline

This folder contains a configurable pipeline that scrapes a government website
for PDF documents, tracks which ones are new, downloads and caches them, turns
them to text, parses spatial detections out of that text, filters detections to a
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
- PDFs with no extractable text layer (image-only scans) or that fail to
  parse are recorded as per-document errors on the state table without
  stopping the rest of the batch. Entries report `"method": "pypdf" |
  "cached"`. (See **Future improvements** for cloud OCR of scanned PDFs.)

### `gazettes_ggmc_parse`: Parse GGMC gazette texts into GeoJSON

- Deterministic (regex) parser adapted from the GGMC Official Gazette spatial
  parser notebook: it segments a document into notice blocks, extracts
  file reference, dates, applicant/transferee, mining district, topographic
  map, acreage, statutory objection deadline, and the `Point A…` boundary
  vertices, converts DMS→decimal degrees (tolerating OCR/typist quirks), and
  builds shapely-validated polygons inside Guyana's bounds.
- Each feature gets a **deterministic id** (`uuid5(doc_url, file_reference,
  block_index)`) so re-runs upsert instead of duplicating.
- **Failure handling (skip and continue):** if any notice block fails to
  parse, the event is logged (`PARSE FAILURE <url>: ...` with the per-block
  `SchemaDriftError` reasons as the diagnosis) and the document is marked
  `error`/`failed` on the state table with the failing blocks in
  `parse_notes`. The broken document is skipped — the run continues with the
  successfully parsed ones (they flow to filtering, the database, and
  notifications as usual), and the failures are also returned in the script
  result. Extend the parser for the new shape, then rerun with
  `include_errored=true` to reprocess failed documents with zero re-download
  or re-extraction.

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
- **Cloud OCR for image-only PDFs.** Scanned gazettes and photographed
  notices have no pypdf text layer and currently error out. A cloud OCR
  (Google Cloud Vision, AWS Textract, Azure Document Intelligence, or the
  hosted Datalab API) would handle these without local model weights — a
  previous in-process option (datalab-to/marker) was removed because its
  torch/model stack (~several GB plus a local inference server) is too heavy
  for Windmill workers. Suggested shape: an `ocr_endpoint`/credentials
  parameter (or a new `gazettes_ocr` script) called from
  `gazettes_pdf_to_text` in place of the error path, storing the OCR result
  in the same cached `.txt`; validate that notice segmentation tolerates the
  returned markdown/plain text before relying on it.

## Keeping Windmill in sync

The Windmill workspace is the source of truth for `f/`: run
`wmill sync push` right after changing these scripts locally. An unpushed
local state is silently reverted by the next `wmill sync pull` (which pulls
whatever the workspace was last saved with).
