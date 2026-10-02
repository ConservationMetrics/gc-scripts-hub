import {
  type DragEvent,
  type ReactNode,
  useEffect,
  useRef,
  useState,
} from "react";

import {
  appendIcon,
  createIcon,
  mergeIcon,
  syncIcon,
} from "./assets/goal-icons";
import { backend } from "./wmill";

type Goal = "create" | "append" | "merge" | "sync";
type Policy = "imported" | "existing";
type Step = "dataset" | "goal" | "identity" | "review" | "upload";
type MetricKind =
  | "added"
  | "columns"
  | "deleted"
  | "total"
  | "unchanged"
  | "updated";

type StagedImport = {
  fields: string[];
  geometry_warning?: string;
  import_id: string;
  record_count: number;
  source_format: string;
};

type Preview = {
  added: number;
  columns_added: number;
  deleted: number;
  final_count: number;
  geometry_invalid?: number;
  geometry_valid?: number;
  unchanged: number;
  updated: number;
  preview_id: string;
};

const MAX_SOURCE_BYTES = 25 * 1024 * 1024;
const acceptedExtensions = [
  "csv",
  "geojson",
  "gpx",
  "gpkg",
  "json",
  "kml",
  "zip",
  "xls",
  "xlsx",
  "xml",
];

const goals: Array<{
  id: Goal;
  icon: string;
  label: string;
  description: string;
}> = [
  {
    id: "create",
    icon: createIcon,
    label: "Create",
    description: "a new dataset from this import.",
  },
  {
    id: "append",
    icon: appendIcon,
    label: "Append",
    description: "every incoming record to an existing dataset.",
  },
  {
    id: "merge",
    icon: mergeIcon,
    label: "Merge",
    description:
      "records into an existing dataset and preserve unmatched records.",
  },
  {
    id: "sync",
    icon: syncIcon,
    label: "Sync",
    description: "an existing dataset, including removal of absent records.",
  },
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object";
}

function usableMessage(value: unknown): string | undefined {
  if (typeof value !== "string" || !value.trim()) return undefined;
  if (value.includes('File "') || value.includes("Traceback")) {
    // Windmill may include the Python stack in the message. Keep its error
    // explanation without exposing stack frames in the user-facing alert.
    return value.match(/^[\w.]+:\s*(.+)$/m)?.[1]?.trim();
  }
  return value.trim();
}

function responseMessage(value: unknown): string | undefined {
  if (typeof value === "string") {
    try {
      const decoded: unknown = JSON.parse(value);
      if (isRecord(decoded)) return responseMessage(decoded);
    } catch {
      // A plain-text error message needs no JSON decoding.
    }
    return usableMessage(value);
  }
  if (!isRecord(value)) return undefined;
  for (const key of ["validation_error", "detail", "message", "error"]) {
    const candidate = value[key];
    const found = responseMessage(candidate);
    if (found) return found;
  }
  return undefined;
}

function message(
  error: unknown,
  fallback = "Something went wrong. Try again.",
) {
  if (isRecord(error)) {
    const fromBody = responseMessage(error.body);
    if (fromBody) return fromBody;
    const fromResponse = responseMessage(error);
    if (fromResponse) return fromResponse;
  }
  const fromError =
    error instanceof Error
      ? usableMessage(error.message)
      : usableMessage(error);
  return fromError ?? fallback;
}

function filePayload(file: File) {
  return new Promise<{ data: string; name: string }>((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () =>
      reject(new Error("The selected file could not be read."));
    reader.onload = () => {
      const result = reader.result;
      if (typeof result !== "string") {
        reject(new Error("The selected file could not be read."));
        return;
      }
      resolve({ data: result.split(",", 2)[1], name: file.name });
    };
    reader.readAsDataURL(file);
  });
}

export default function App() {
  const [step, setStep] = useState<Step>("goal");
  const [goal, setGoal] = useState<Goal>();
  const [datasets, setDatasets] = useState<string[]>([]);
  const [datasetsLoading, setDatasetsLoading] = useState(true);
  const [datasetsError, setDatasetsError] = useState<string>();
  const [datasetName, setDatasetName] = useState("");
  const [targetTable, setTargetTable] = useState("");
  const [nameAvailable, setNameAvailable] = useState<boolean | undefined>();
  const [nameChecking, setNameChecking] = useState(false);
  const [file, setFile] = useState<File>();
  const [staged, setStaged] = useState<StagedImport>();
  const [identity, setIdentity] = useState<string[]>([]);
  const [policy, setPolicy] = useState<Policy>("imported");
  const [preview, setPreview] = useState<Preview>();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();
  const [success, setSuccess] = useState(false);
  const [dragging, setDragging] = useState(false);
  const headingRef = useRef<HTMLHeadingElement>(null);

  async function loadDatasets() {
    setDatasetsLoading(true);
    setDatasetsError(undefined);
    try {
      setDatasets(await backend.list_datasets());
    } catch (reason) {
      setDatasetsError(message(reason, "We could not load the dataset list."));
    } finally {
      setDatasetsLoading(false);
    }
  }

  useEffect(() => {
    let active = true;
    backend
      .list_datasets()
      .then((result) => {
        if (active) setDatasets(result);
      })
      .catch((reason) => {
        if (active) {
          setDatasetsError(
            message(reason, "We could not load the dataset list."),
          );
        }
      })
      .finally(() => {
        if (active) setDatasetsLoading(false);
      });
    return () => {
      active = false;
    };
  }, []);
  useEffect(() => {
    headingRef.current?.focus();
  }, [step]);

  useEffect(() => {
    if (goal !== "create" || !datasetName.trim()) {
      return;
    }
    let active = true;
    const timer = window.setTimeout(() => {
      backend
        .check_dataset_name({ dataset_name: datasetName })
        .then((result) => {
          if (!active) return;
          setNameAvailable(result.available);
          setTargetTable(result.table_name);
          setNameChecking(false);
          setError(undefined);
        })
        .catch((reason) => {
          if (!active) return;
          setNameChecking(false);
          setError(message(reason, "We could not check this dataset name."));
        });
    }, 350);
    return () => {
      active = false;
      window.clearTimeout(timer);
    };
  }, [datasetName, goal]);

  const target = targetTable;
  const needsIdentity = goal === "merge" || goal === "sync";
  const steps: Array<{ id: Step; label: string }> = [
    { id: "goal", label: "Goal" },
    { id: "dataset", label: "Dataset" },
    { id: "upload", label: "Upload" },
    ...(needsIdentity ? [{ id: "identity" as Step, label: "Identity" }] : []),
    { id: "review", label: "Review" },
  ];
  const currentIndex = steps.findIndex((item) => item.id === step);

  function clearStaged() {
    setStaged(undefined);
    setIdentity([]);
    setPolicy("imported");
    setPreview(undefined);
    setSuccess(false);
  }

  function clearUpload() {
    setFile(undefined);
    clearStaged();
  }

  function resetImport() {
    setStep("goal");
    setGoal(undefined);
    setDatasetName("");
    setTargetTable("");
    setNameAvailable(undefined);
    setNameChecking(false);
    setFile(undefined);
    setStaged(undefined);
    setIdentity([]);
    setPolicy("imported");
    setPreview(undefined);
    setError(undefined);
    setSuccess(false);
  }

  function chooseGoal(nextGoal: Goal) {
    setGoal(nextGoal);
    setStep("goal");
    setTargetTable("");
    setDatasetName("");
    setNameAvailable(undefined);
    setNameChecking(false);
    clearUpload();
    setError(undefined);
  }

  function selectFile(nextFile?: File) {
    setError(undefined);
    if (!nextFile) {
      clearUpload();
      return;
    }
    const extension = nextFile.name.split(".").pop()?.toLowerCase() ?? "";
    if (!acceptedExtensions.includes(extension)) {
      clearUpload();
      setError(`Choose a supported file (${acceptedExtensions.join(", ")}).`);
      return;
    }
    if (nextFile.size > MAX_SOURCE_BYTES) {
      clearUpload();
      setError("Source uploads must not exceed 25 MiB.");
      return;
    }
    setFile(nextFile);
    clearStaged();
  }

  function dropFile(event: DragEvent<HTMLLabelElement>) {
    event.preventDefault();
    setDragging(false);
    selectFile(event.dataTransfer.files[0]);
  }

  async function stage() {
    if (!file || !target || !goal) return;
    setLoading(true);
    setError(undefined);
    try {
      const result = await backend.stage_import({
        goal,
        target_table: target,
        uploaded_file: await filePayload(file),
      });
      if ("validation_error" in result) {
        setError(result.validation_error);
        return;
      }
      setStaged(result);
      setPreview(undefined);
      setIdentity([]);
      setStep(needsIdentity ? "identity" : "review");
    } catch (reason) {
      setError(message(reason));
    } finally {
      setLoading(false);
    }
  }

  async function makePreview() {
    if (!staged) return;
    setLoading(true);
    setError(undefined);
    try {
      const result = await backend.preview_import({
        import_id: staged.import_id,
        identity_fields: identity,
        update_policy: policy,
      });
      if ("validation_error" in result) {
        setError(result.validation_error);
        return;
      }
      setPreview(result);
      setStep("review");
    } catch (reason) {
      setError(
        message(
          reason,
          "We could not preview this import. Try again. If the problem persists, report it with the upload filename and selected identity fields.",
        ),
      );
    } finally {
      setLoading(false);
    }
  }

  async function confirm() {
    if (!staged || !preview) return;
    if (
      goal === "sync" &&
      preview.deleted > 0 &&
      !window.confirm(
        `Sync will delete ${preview.deleted} record${preview.deleted === 1 ? "" : "s"} from ${target}. Continue?`,
      )
    )
      return;
    setLoading(true);
    setError(undefined);
    try {
      await backend.apply_import({
        import_id: staged.import_id,
        preview_id: preview.preview_id,
      });
      if (goal === "create") {
        setDatasets((current) => [...new Set([...current, target])].sort());
      }
      setSuccess(true);
    } catch (reason) {
      setError(message(reason));
      if (message(reason).toLowerCase().includes("review"))
        setPreview(undefined);
    } finally {
      setLoading(false);
    }
  }

  const canMoveDataset =
    goal === "create" ? nameAvailable === true : Boolean(targetTable);
  const canPreviewIdentity = Boolean(
    staged && (staged.record_count === 0 || identity.length > 0),
  );
  const isNoop =
    preview &&
    preview.added === 0 &&
    preview.updated === 0 &&
    preview.deleted === 0 &&
    preview.columns_added === 0;
  const nameStatus = nameChecking
    ? "Checking availability..."
    : nameAvailable === true
      ? `This name is available as ${targetTable}.`
      : nameAvailable === false
        ? "A dataset with this name already exists."
        : "";

  return (
    <main className="app-shell">
      <header>
        <h1>Dataset importer</h1>
      </header>

      {error && (
        <div className="notice error" role="alert">
          {error}
        </div>
      )}

      {step === "goal" && (
        <section aria-labelledby="step-heading">
          <h2
            className="goal-heading"
            id="step-heading"
            ref={headingRef}
            tabIndex={-1}
          >
            What's your goal?
          </h2>
          <div aria-label="Import goal" className="goal-grid" role="radiogroup">
            {goals.map((option) => (
              <label
                className={`goal-card ${goal === option.id ? "selected" : ""}`}
                key={option.id}
              >
                <input
                  checked={goal === option.id}
                  name="goal"
                  onChange={() => chooseGoal(option.id)}
                  type="radio"
                />
                <img alt="" src={option.icon} />
                <span>
                  <strong>{option.label}</strong> {option.description}
                </span>
              </label>
            ))}
          </div>
        </section>
      )}

      {step === "dataset" && (
        <section aria-labelledby="step-heading">
          <h2 id="step-heading" ref={headingRef} tabIndex={-1}>
            {goal === "create"
              ? "Name your new dataset"
              : "Select target dataset"}
          </h2>
          {goal === "create" ? (
            <label className="field">
              Dataset name
              <input
                aria-describedby="dataset-name-status"
                aria-invalid={nameAvailable === false}
                onChange={(event) => {
                  const value = event.target.value;
                  setDatasetName(value);
                  setNameAvailable(undefined);
                  setNameChecking(Boolean(value.trim()));
                  setTargetTable("");
                  clearUpload();
                }}
                placeholder="Bird observations"
                value={datasetName}
              />
              {datasetName && (
                <small
                  aria-live="polite"
                  className={
                    nameAvailable === false
                      ? "invalid"
                      : nameAvailable
                        ? "valid"
                        : ""
                  }
                  id="dataset-name-status"
                  role="status"
                >
                  {nameStatus}
                </small>
              )}
            </label>
          ) : (
            <label className="field">
              Dataset
              <select
                disabled={datasetsLoading}
                onChange={(event) => {
                  setTargetTable(event.target.value);
                  clearUpload();
                }}
                value={targetTable}
              >
                <option value="">
                  {datasetsLoading ? "Loading datasets..." : "Choose a dataset"}
                </option>
                {datasets.map((dataset) => (
                  <option key={dataset} value={dataset}>
                    {dataset}
                  </option>
                ))}
              </select>
              {!datasetsLoading && datasets.length === 0 && !datasetsError && (
                <small>No compatible datasets are available.</small>
              )}
              {datasetsError && (
                <small className="invalid" role="alert">
                  {datasetsError}
                </small>
              )}
              {datasetsError && (
                <button
                  className="link-button"
                  onClick={() => void loadDatasets()}
                  type="button"
                >
                  Retry dataset list
                </button>
              )}
            </label>
          )}
        </section>
      )}

      {step === "upload" && (
        <section aria-labelledby="step-heading">
          <h2 id="step-heading" ref={headingRef} tabIndex={-1}>
            Upload your data
          </h2>
          <p className="lede">
            CSV uploads can be tab-delimited. JSON uploads can contain arrays or
            CyberTracker backups. XML uploads must be SMART patrol exports. ZIP
            archives can contain a Shapefile or supported files.
          </p>
          <label
            className={`dropzone ${dragging ? "dragging" : ""}`}
            onDragEnter={(event) => {
              event.preventDefault();
              setDragging(true);
            }}
            onDragLeave={() => setDragging(false)}
            onDragOver={(event) => event.preventDefault()}
            onDrop={dropFile}
          >
            <input
              accept={acceptedExtensions
                .map((extension) => `.${extension}`)
                .join(",")}
              onChange={(event) => {
                selectFile(event.target.files?.[0]);
                event.target.value = "";
              }}
              type="file"
            />
            <strong>
              {file ? file.name : "Drop a file here, or click to browse"}
            </strong>
            <span>
              {file ? (
                `${(file.size / 1024 / 1024).toFixed(2)} MiB`
              ) : (
                <>
                  Supported formats: {acceptedExtensions.join(", ")}. <br />
                  Maximum source size: 25 MiB. GeoPackages must contain one
                  spatial layer.
                </>
              )}
            </span>
          </label>
          <p>
            To upload media attachments, such as photos, use File Browser. See
            the{" "}
            <a href="https://docs.guardianconnector.net/reference/gc-toolkit/gc-scripts-hub/dataset-importer">
              documentation
            </a>
            .
          </p>
        </section>
      )}

      {step === "identity" && needsIdentity && staged && (
        <section aria-labelledby="step-heading">
          <h2 id="step-heading" ref={headingRef} tabIndex={-1}>
            Choose record identity
          </h2>
          <p className="lede">
            Choose one to three columns to match rows in your file to existing
            records. For example, use a record ID, or a combination of site and
            observation date. All selected values must match for two rows to
            count as the same record.
          </p>
          <p>
            Choose values that stay the same when a record is updated. Their
            combination must be unique for each record in both your file and the
            dataset.
          </p>
          <p>
            Matching rows follow the update policy below. Uploaded rows without
            a match are added as new records.{" "}
            {goal === "sync"
              ? "Sync also deletes existing records that have no match in your file."
              : "Merge keeps existing records that have no match in your file."}
          </p>
          <div className="identity-fields">
            {[0, 1, 2].map((position) => (
              <select
                aria-label={`Identity field ${position + 1}`}
                disabled={position > identity.length}
                key={position}
                onChange={(event) => {
                  const next = identity.slice(0, position);
                  if (event.target.value) next.push(event.target.value);
                  setIdentity(next);
                  setPreview(undefined);
                }}
                value={identity[position] ?? ""}
              >
                <option value="">
                  {position === 0
                    ? "Choose a field"
                    : "Add another field (optional)"}
                </option>
                {staged.fields
                  .filter(
                    (field) =>
                      !identity.includes(field) || identity[position] === field,
                  )
                  .map((field) => (
                    <option key={field} value={field}>
                      {field}
                    </option>
                  ))}
              </select>
            ))}
          </div>
          <fieldset>
            <legend>Update policy</legend>
            <label>
              <input
                checked={policy === "imported"}
                name="policy"
                onChange={() => {
                  setPolicy("imported");
                  setPreview(undefined);
                }}
                type="radio"
              />{" "}
              Imported records win, including empty values.
            </label>
            <label>
              <input
                checked={policy === "existing"}
                name="policy"
                onChange={() => {
                  setPolicy("existing");
                  setPreview(undefined);
                }}
                type="radio"
              />{" "}
              Existing records win; matching rows stay unchanged.
            </label>
          </fieldset>
        </section>
      )}

      {step === "review" && (
        <section aria-labelledby="step-heading">
          <h2 id="step-heading" ref={headingRef} tabIndex={-1}>
            Review import
          </h2>
          {!preview && (
            <p className="lede">
              Generate a preview before writing to the dataset.
            </p>
          )}
          <dl className="review-summary">
            <div>
              <dt>Goal</dt>
              <dd>{goal}</dd>
            </div>
            <div>
              <dt>Dataset</dt>
              <dd>{target}</dd>
            </div>
            <div>
              <dt>Source</dt>
              <dd>{file?.name}</dd>
            </div>
            {staged && (
              <div>
                <dt>Detected format</dt>
                <dd>{staged.source_format}</dd>
              </div>
            )}
            {needsIdentity && identity.length > 0 && (
              <div>
                <dt>Identity</dt>
                <dd>{identity.join(" + ")}</dd>
              </div>
            )}
            {needsIdentity && (
              <div>
                <dt>Update policy</dt>
                <dd>
                  {policy === "imported"
                    ? "Imported records win"
                    : "Existing records win"}
                </dd>
              </div>
            )}
          </dl>
          {staged?.geometry_warning && (
            <div className="notice warning" role="status">
              {staged.geometry_warning}
            </div>
          )}
          {preview && (
            <div className="review-grid">
              <Metric
                kind="deleted"
                label="Rows deleted"
                value={preview.deleted}
              />
              <Metric
                kind="updated"
                label="Rows updated"
                value={preview.updated}
              />
              <Metric kind="added" label="Rows added" value={preview.added} />
              <Metric
                kind="columns"
                label="Columns added"
                value={preview.columns_added}
              />
              <Metric
                kind="unchanged"
                label="Unchanged records"
                value={preview.unchanged}
              />
              <Metric
                kind="total"
                label="Final record count"
                value={preview.final_count}
              />
            </div>
          )}
          {preview?.geometry_valid !== undefined &&
            preview.geometry_invalid !== undefined && (
              <div
                className={`notice ${preview.geometry_invalid > 0 ? "warning" : "neutral"}`}
                role="status"
              >
                {preview.geometry_valid} of{" "}
                {preview.geometry_valid + preview.geometry_invalid} imported
                records include valid map geometry.
                {preview.geometry_invalid > 0 &&
                  ` ${preview.geometry_invalid} imported record${preview.geometry_invalid === 1 ? " has" : "s have"} no valid map geometry.`}
              </div>
            )}
          {preview && goal === "sync" && preview.deleted > 0 && (
            <div className="notice warning" role="status">
              Sync will permanently delete {preview.deleted} unmatched record
              {preview.deleted === 1 ? "" : "s"} from {target}.
            </div>
          )}
          {staged?.source_format === "zip" && goal === "create" && (
            <div className="notice neutral" role="status">
              Files in this archive are appended in archive order. Rows are not
              matched or deduplicated.
            </div>
          )}
          {isNoop && (
            <div className="notice neutral" role="status">
              This import makes no dataset changes.
            </div>
          )}
          {success && (
            <div aria-live="polite" className="notice success" role="status">
              Import applied successfully.
            </div>
          )}
        </section>
      )}

      <footer>
        <ol className="progress" aria-label="Import progress">
          {steps.map((item, index) => (
            <li
              aria-current={step === item.id ? "step" : undefined}
              className={currentIndex >= index ? "active" : ""}
              key={item.id}
            >
              <span>{index + 1}</span>
              <small>{item.label}</small>
            </li>
          ))}
        </ol>
        <div className="actions">
          <button
            disabled={currentIndex === 0 || loading || success}
            onClick={() => {
              setStep(steps[currentIndex - 1].id);
              setError(undefined);
            }}
            type="button"
          >
            Back
          </button>
          {step === "goal" && (
            <button
              className="primary"
              disabled={!goal}
              onClick={() => setStep("dataset")}
              type="button"
            >
              Next
            </button>
          )}
          {step === "dataset" && (
            <button
              className="primary"
              disabled={!canMoveDataset}
              onClick={() => setStep("upload")}
              type="button"
            >
              Next
            </button>
          )}
          {step === "upload" && (
            <button
              className="primary"
              disabled={!file || !target || loading}
              onClick={stage}
              type="button"
            >
              {loading ? "Staging..." : "Stage upload"}
            </button>
          )}
          {step === "identity" && (
            <button
              className="primary"
              disabled={!canPreviewIdentity || loading}
              onClick={makePreview}
              type="button"
            >
              {loading ? "Calculating..." : "Preview changes"}
            </button>
          )}
          {step === "review" && !preview && (
            <button
              className="primary"
              disabled={loading}
              onClick={makePreview}
              type="button"
            >
              {loading ? "Calculating..." : "Preview changes"}
            </button>
          )}
          {step === "review" && preview && !success && (
            <button
              className="primary"
              disabled={loading}
              onClick={confirm}
              type="button"
            >
              {loading ? "Importing..." : "Confirm import"}
            </button>
          )}
          {step === "review" && success && (
            <button className="primary" onClick={resetImport} type="button">
              Import another dataset
            </button>
          )}
        </div>
      </footer>
    </main>
  );
}

function Metric({
  kind,
  label,
  value,
}: {
  kind: MetricKind;
  label: string;
  value: number;
}) {
  return (
    <article className={`metric ${value > 0 ? `active ${kind}` : "zero"}`}>
      <MetricIcon kind={kind} />
      <div>
        <strong>{value}</strong>
        <span>{label}</span>
      </div>
    </article>
  );
}

function MetricIcon({ kind }: { kind: MetricKind }) {
  const paths: Record<MetricKind, ReactNode> = {
    added: <path d="M12 5v14M5 12h14" />,
    columns: (
      <>
        <rect height="14" rx="1" width="14" x="5" y="5" />
        <path d="M12 5v14M5 12h14" />
      </>
    ),
    deleted: (
      <>
        <path d="M4 7h16M9 7V4h6v3M7 7l1 13h8l1-13" />
        <path d="M10 11v5M14 11v5" />
      </>
    ),
    total: (
      <>
        <ellipse cx="12" cy="6" rx="7" ry="3" />
        <path d="M5 6v6c0 1.7 3.1 3 7 3s7-1.3 7-3V6M5 12v6c0 1.7 3.1 3 7 3s7-1.3 7-3v-6" />
      </>
    ),
    unchanged: <path d="m5 12 4 4L19 6" />,
    updated: (
      <>
        <path d="m4 20 4.5-1 10-10-3.5-3.5-10 10L4 20Z" />
        <path d="m13.5 7 3.5 3.5" />
      </>
    ),
  };
  return (
    <span className="metric-icon" aria-hidden="true">
      <svg
        fill="none"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="2"
        viewBox="0 0 24 24"
      >
        {paths[kind]}
      </svg>
    </span>
  );
}
