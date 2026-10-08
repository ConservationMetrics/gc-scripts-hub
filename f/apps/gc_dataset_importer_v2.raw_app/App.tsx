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
import strings from "./strings.json";
import { backend } from "./wmill";

type Goal = "create" | "append" | "merge" | "sync";
type Policy = "imported" | "existing";
type Step = "dataset" | "goal" | "identity" | "review" | "upload";
type MetricKind =
  "added" | "columns" | "deleted" | "total" | "unchanged" | "updated";

type StagedImport = {
  eligible_identity_fields: string[];
  fields: string[];
  source_mapping: Record<string, string>;
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
    label: strings.createLabel,
    description: strings.createDescription,
  },
  {
    id: "append",
    icon: appendIcon,
    label: strings.appendLabel,
    description: strings.appendDescription,
  },
  {
    id: "merge",
    icon: mergeIcon,
    label: strings.mergeLabel,
    description: strings.mergeDescription,
  },
  {
    id: "sync",
    icon: syncIcon,
    label: strings.syncLabel,
    description: strings.syncDescription,
  },
];

function format(template: string, values: Record<string, string | number>) {
  return template.replace(/\{(\w+)\}/g, (placeholder, key: string) =>
    values[key] === undefined ? placeholder : String(values[key]),
  );
}

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

function message(error: unknown, fallback = strings.errorGeneric) {
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
    reader.onerror = () => reject(new Error(strings.errorReadFile));
    reader.onload = () => {
      const result = reader.result;
      if (typeof result !== "string") {
        reject(new Error(strings.errorReadFile));
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
      setDatasets(await backend.list_datasets({}));
    } catch (reason) {
      setDatasetsError(message(reason, strings.errorLoadDatasets));
    } finally {
      setDatasetsLoading(false);
    }
  }

  useEffect(() => {
    let active = true;
    backend
      .list_datasets({})
      .then((result) => {
        if (active) setDatasets(result);
      })
      .catch((reason) => {
        if (active) {
          setDatasetsError(message(reason, strings.errorLoadDatasets));
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
          setError(message(reason, strings.errorCheckName));
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
    { id: "goal", label: strings.stepGoal },
    { id: "dataset", label: strings.stepDataset },
    { id: "upload", label: strings.stepUpload },
    ...(needsIdentity
      ? [{ id: "identity" as Step, label: strings.stepIdentity }]
      : []),
    { id: "review", label: strings.stepReview },
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
      setError(
        format(strings.errorUnsupported, {
          formats: acceptedExtensions.join(", "),
        }),
      );
      return;
    }
    if (nextFile.size > MAX_SOURCE_BYTES) {
      clearUpload();
      setError(strings.errorFileTooLarge);
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
      console.error("Failed to stage import", reason);
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
      console.error("Failed to preview import", reason);
      setError(message(reason, strings.errorPreview));
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
        format(
          preview.deleted === 1
            ? strings.syncConfirmOne
            : strings.syncConfirmOther,
          { count: preview.deleted, dataset: target },
        ),
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
      console.error("Failed to apply import", reason);
      setError(message(reason));
      if (message(reason).toLowerCase().includes("review"))
        setPreview(undefined);
    } finally {
      setLoading(false);
    }
  }

  const canMoveDataset =
    goal === "create" ? nameAvailable === true : Boolean(targetTable);
  function fieldLabel(field: string) {
    const stored = staged?.source_mapping[field];
    return stored && stored !== field ? `${field} → ${stored}` : field;
  }

  const sortedIdentityFields = staged
    ? [...staged.fields].sort(
        (left, right) =>
          Number(staged.eligible_identity_fields.includes(right)) -
            Number(staged.eligible_identity_fields.includes(left)) ||
          fieldLabel(left).localeCompare(fieldLabel(right), undefined, {
            sensitivity: "base",
          }),
      )
    : [];

  const noEligibleIdentity = Boolean(
    staged &&
    staged.record_count > 0 &&
    staged.eligible_identity_fields.length === 0,
  );
  const canPreviewIdentity = Boolean(
    staged &&
    (staged.record_count === 0 ||
      (identity.length > 0 &&
        identity.every((field) =>
          staged.eligible_identity_fields.includes(field),
        ))),
  );
  const isNoop =
    preview &&
    preview.added === 0 &&
    preview.updated === 0 &&
    preview.deleted === 0 &&
    preview.columns_added === 0;
  const nameStatus = nameChecking
    ? strings.nameChecking
    : nameAvailable === true
      ? format(strings.nameAvailable, { dataset: targetTable })
      : nameAvailable === false
        ? strings.nameUnavailable
        : "";

  return (
    <main className="app-shell">
      <header>
        <h1>{strings.appTitle}</h1>
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
            {strings.goalHeading}
          </h2>
          <div
            aria-label={strings.goalAriaLabel}
            className="goal-grid"
            role="radiogroup"
          >
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
            {goal === "create" ? strings.nameHeading : strings.datasetHeading}
          </h2>
          {goal === "create" ? (
            <label className="field">
              {strings.datasetNameLabel}
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
                placeholder={strings.datasetPlaceholder}
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
              {strings.stepDataset}
              <select
                disabled={datasetsLoading}
                onChange={(event) => {
                  setTargetTable(event.target.value);
                  clearUpload();
                }}
                value={targetTable}
              >
                <option value="">
                  {datasetsLoading
                    ? strings.datasetsLoading
                    : strings.datasetChoose}
                </option>
                {datasets.map((dataset) => (
                  <option key={dataset} value={dataset}>
                    {dataset}
                  </option>
                ))}
              </select>
              {!datasetsLoading && datasets.length === 0 && !datasetsError && (
                <small>{strings.datasetsEmpty}</small>
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
                  {strings.actionRetryDatasets}
                </button>
              )}
            </label>
          )}
        </section>
      )}

      {step === "upload" && (
        <section aria-labelledby="step-heading">
          <h2 id="step-heading" ref={headingRef} tabIndex={-1}>
            {strings.uploadHeading}
          </h2>
          <p className="lede">{strings.uploadDescription}</p>
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
            <strong>{file ? file.name : strings.fileDrop}</strong>
            <span>
              {file ? (
                format(strings.fileSize, {
                  size: (file.size / 1024 / 1024).toFixed(2),
                })
              ) : (
                <>
                  {format(strings.uploadFormats, {
                    formats: acceptedExtensions.join(", "),
                  })}{" "}
                  <br />
                  {strings.uploadLimits}
                </>
              )}
            </span>
          </label>
          <p>
            {strings.uploadMedia}{" "}
            <a href="https://docs.guardianconnector.net/reference/gc-toolkit/gc-scripts-hub/dataset-importer">
              {strings.uploadDocumentation}
            </a>
            .
          </p>
          {loading && (
            <div
              aria-label={strings.actionStaging}
              className="staging-progress"
              role="progressbar"
            />
          )}
        </section>
      )}

      {step === "identity" && needsIdentity && staged && (
        <section aria-labelledby="step-heading">
          <h2 id="step-heading" ref={headingRef} tabIndex={-1}>
            {strings.identityHeading}
          </h2>
          <p className="lede">{strings.identityDescription}</p>
          <p>{strings.identityUnique}</p>
          {goal === "sync" && <p>{strings.identitySync}</p>}
          <div className="identity-fields">
            {[0, 1, 2].map((position) => (
              <select
                aria-label={format(strings.identityField, {
                  number: position + 1,
                })}
                disabled={noEligibleIdentity || position > identity.length}
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
                    ? strings.identityChoose
                    : strings.identityAdd}
                </option>
                {sortedIdentityFields
                  .filter(
                    (field) =>
                      !identity.includes(field) || identity[position] === field,
                  )
                  .map((field) => (
                    <option
                      disabled={
                        !staged.eligible_identity_fields.includes(field)
                      }
                      key={field}
                      value={field}
                    >
                      {fieldLabel(field)}
                      {!staged.eligible_identity_fields.includes(field) &&
                        ` — ${strings.identityNotInTarget}`}
                    </option>
                  ))}
              </select>
            ))}
          </div>
          {noEligibleIdentity && (
            <p role="status">{strings.identityNoEligibleFields}</p>
          )}
          <fieldset>
            <legend>{strings.policyLabel}</legend>
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
              {strings.policyImported}
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
              {strings.policyExisting}
            </label>
          </fieldset>
        </section>
      )}

      {step === "review" && (
        <section aria-labelledby="step-heading">
          <h2 id="step-heading" ref={headingRef} tabIndex={-1}>
            {strings.reviewHeading}
          </h2>
          {!preview && <p className="lede">{strings.reviewDescription}</p>}
          {preview && !success && (
            <p className="lede">{strings.reviewProposedChanges}</p>
          )}
          <dl className="review-summary">
            <div>
              <dt>{strings.stepGoal}</dt>
              <dd>{goals.find((option) => option.id === goal)?.label}</dd>
            </div>
            <div>
              <dt>{strings.stepDataset}</dt>
              <dd>{target}</dd>
            </div>
            <div>
              <dt>{strings.reviewSource}</dt>
              <dd>{file?.name}</dd>
            </div>
            {staged && (
              <div>
                <dt>{strings.reviewFormat}</dt>
                <dd>{staged.source_format}</dd>
              </div>
            )}
            {needsIdentity && identity.length > 0 && (
              <div>
                <dt>{strings.stepIdentity}</dt>
                <dd>{identity.map(fieldLabel).join(" + ")}</dd>
              </div>
            )}
            {needsIdentity && (
              <div>
                <dt>{strings.policyLabel}</dt>
                <dd>
                  {policy === "imported"
                    ? strings.policyImportedSummary
                    : strings.policyExistingSummary}
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
                label={strings.metricDeleted}
                value={preview.deleted}
              />
              <Metric
                kind="updated"
                label={strings.metricUpdated}
                value={preview.updated}
              />
              <Metric
                kind="added"
                label={strings.metricAdded}
                value={preview.added}
              />
              <Metric
                kind="columns"
                label={strings.metricColumns}
                value={preview.columns_added}
              />
              <Metric
                kind="unchanged"
                label={strings.metricUnchanged}
                value={preview.unchanged}
              />
              <Metric
                kind="total"
                label={strings.metricTotal}
                value={preview.final_count}
              />
            </div>
          )}
          {preview && staged && staged.fields.length > 0 && (
            <details className="field-mappings">
              <summary>{strings.reviewMapping}</summary>
              <div className="field-mappings-scroll">
                <table aria-label={strings.reviewMapping}>
                  <thead>
                    <tr>
                      <th scope="col">{strings.mappingSource}</th>
                      <th scope="col">{strings.mappingStored}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {staged.fields.map((field) => (
                      <tr key={field}>
                        <td>{field}</td>
                        <td>{staged.source_mapping[field]}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </details>
          )}
          {preview?.geometry_valid !== undefined &&
            preview.geometry_invalid !== undefined && (
              <div
                className={`notice ${preview.geometry_invalid > 0 ? "warning" : "neutral"}`}
                role="status"
              >
                {format(strings.geometryValid, {
                  valid: preview.geometry_valid,
                  total: preview.geometry_valid + preview.geometry_invalid,
                })}
                {preview.geometry_invalid > 0 && (
                  <>
                    {" "}
                    {format(
                      preview.geometry_invalid === 1
                        ? strings.geometryInvalidOne
                        : strings.geometryInvalidOther,
                      { count: preview.geometry_invalid },
                    )}
                  </>
                )}
              </div>
            )}
          {preview && goal === "sync" && preview.deleted > 0 && (
            <div className="notice warning" role="status">
              {format(
                preview.deleted === 1
                  ? strings.syncWarningOne
                  : strings.syncWarningOther,
                { count: preview.deleted, dataset: target },
              )}
            </div>
          )}
          {staged?.source_format === "zip" && goal === "create" && (
            <div className="notice neutral" role="status">
              {strings.reviewZip}
            </div>
          )}
          {isNoop && (
            <div className="notice neutral" role="status">
              {strings.reviewNoChanges}
            </div>
          )}
          {success && (
            <div aria-live="polite" className="notice success" role="status">
              {strings.reviewSuccess}
            </div>
          )}
        </section>
      )}

      <footer>
        <ol className="progress" aria-label={strings.progressAriaLabel}>
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
            {strings.actionBack}
          </button>
          {step === "goal" && (
            <button
              className="primary"
              disabled={!goal}
              onClick={() => setStep("dataset")}
              type="button"
            >
              {strings.actionNext}
            </button>
          )}
          {step === "dataset" && (
            <button
              className="primary"
              disabled={!canMoveDataset}
              onClick={() => setStep("upload")}
              type="button"
            >
              {strings.actionNext}
            </button>
          )}
          {step === "upload" && (
            <button
              className="primary"
              disabled={!file || !target || loading}
              onClick={stage}
              type="button"
            >
              {loading ? strings.actionStaging : strings.actionStage}
            </button>
          )}
          {step === "identity" && (
            <button
              className="primary"
              disabled={!canPreviewIdentity || loading}
              onClick={makePreview}
              type="button"
            >
              {loading ? strings.actionCalculating : strings.actionPreview}
            </button>
          )}
          {step === "review" && !preview && (
            <button
              className="primary"
              disabled={loading}
              onClick={makePreview}
              type="button"
            >
              {loading ? strings.actionCalculating : strings.actionPreview}
            </button>
          )}
          {step === "review" && preview && !success && (
            <button
              className="primary"
              disabled={loading}
              onClick={confirm}
              type="button"
            >
              {loading ? strings.actionImporting : strings.actionConfirm}
            </button>
          )}
          {step === "review" && success && (
            <button className="primary" onClick={resetImport} type="button">
              {strings.actionAnother}
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
