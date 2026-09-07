# Gazettes: Scrape, download, process, and notify (GGMC)

This flow chains the gazettes pipeline scripts for the Guyana Geology and Mines
Commission gazettes site:

1. `gazettes_scrape`: Scrape the site for PDF links and track updates
2. `gazettes_download`: Download new/changed PDFs into the local cache
3. `gazettes_pdf_to_text`: Convert the PDFs to (cached) text files, with an
   OCR fallback (marker, in-process) for image-only or unreadable PDFs
4. `gazettes_ggmc_parse`: Parse gazette notices into GeoJSON detections with
   the deterministic parser; unparseable notice blocks are logged, recorded on
   the documents state table, and fail the run for review
5. `gazettes_filter_by_boundary`: (optional) keep only detections inside a
   territory boundary file
6. `geojson_to_postgres`: load the detections table
7. `alerts_twilio`: (optional) send a WhatsApp summary of the new detections

Modules are skipped when there is nothing to do (no new documents, no boundary
path, no Twilio resource), following the same pattern as
`alerts_download_post_notify.flow`.

> [!WARNING]
> **Marker model weights (OCR step):** the first OCR run downloads several GB
> of weights and spawns a local inference server. Before scheduling this flow
> with `ocr_fallback` enabled, point `MODEL_DIR`/`HF_HOME` at
> `/persistent-storage` on the Windmill workers and make sure the inference
> backend (llama.cpp `llama-server` on CPU hosts, vLLM on GPU hosts) is
> available — otherwise scanned/unreadable PDFs are recorded as errors. See
> the marker worker prerequisites in `f/connectors/gazettes/README.md`.

## Key inputs

- `site_url` / `pdf_selector`: what to scrape (defaults target the GGMC gazettes
  page; the selector is scoped to the page content area).
- `db` (postgresql resource) + `db_table_name` (e.g. `ggmc_notices`): where
  detections and scrape state live.
- `cache_dir` / `destination_path`: data lake locations for the PDF/text cache
  and the intermediate GeoJSON.
- `ocr_fallback`: OCR image-only/unreadable PDFs with marker during text
  extraction (default true; requires marker's model weights and inference
  backend on the worker — see the gazettes connector README).
- `boundary_path`: optional GeoJSON boundary in the data lake; when set, only
  intersecting detections are loaded and notified.
- `include_errored`: flip to true for one run to retry previously failed
  documents after fixing the parser.
- `instance_slug` + `twilio_message_template`: required for the WhatsApp step.
