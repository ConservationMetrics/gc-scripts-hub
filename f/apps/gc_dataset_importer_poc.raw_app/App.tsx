import { useEffect, useState } from "react";

import { backend } from "./wmill";

type Goal = "create" | "append" | "merge" | "sync";
type Policy = "imported" | "existing";

type StagedImport = {
  fields: string[];
  import_id: string;
  record_count: number;
  source_format: string;
};

type Preview = {
  added: number;
  columns_added: number;
  deleted: number;
  final_count: number;
  unchanged: number;
  updated: number;
};

const goals: Array<{ id: Goal; title: string; description: string }> = [
  {
    id: "create",
    title: "Create a new dataset",
    description: "Start a new dataset from this import.",
  },
  {
    id: "append",
    title: "Append to an existing dataset",
    description: "Add every incoming record without matching existing records.",
  },
  {
    id: "merge",
    title: "Merge into an existing dataset",
    description: "Match records, add new ones, and keep records not in the import.",
  },
  {
    id: "sync",
    title: "Sync an existing dataset",
    description: "Match records, add new ones, and remove records not in the import.",
  },
];

function message(error: unknown) {
  if (error instanceof Error) return error.message;
  return typeof error === "string" ? error : "Something went wrong. Try again.";
}

function filePayload(file: File) {
  return new Promise<{ data: string; name: string }>((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error("The selected file could not be read."));
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
  const [step, setStep] = useState(0);
  const [goal, setGoal] = useState<Goal>("create");
  const [datasets, setDatasets] = useState<string[]>([]);
  const [datasetName, setDatasetName] = useState("");
  const [targetTable, setTargetTable] = useState("");
  const [nameAvailable, setNameAvailable] = useState<boolean | undefined>();
  const [file, setFile] = useState<File>();
  const [staged, setStaged] = useState<StagedImport>();
  const [identity, setIdentity] = useState<string[]>([]);
  const [policy, setPolicy] = useState<Policy>("imported");
  const [preview, setPreview] = useState<Preview>();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();
  const [success, setSuccess] = useState(false);

  useEffect(() => {
    backend.list_datasets().then(setDatasets).catch((reason) => setError(message(reason)));
  }, []);

  useEffect(() => {
    if (goal !== "create" || !datasetName.trim()) {
      setNameAvailable(undefined);
      return;
    }
    let active = true;
    setNameAvailable(undefined);
    const timer = window.setTimeout(() => {
      backend
        .check_dataset_name({ dataset_name: datasetName })
        .then((result) => {
          if (!active) return;
          setNameAvailable(result.available);
          setTargetTable(result.table_name);
        })
        .catch((reason) => active && setError(message(reason)));
    }, 350);
    return () => {
      active = false;
      window.clearTimeout(timer);
    };
  }, [datasetName, goal]);

  const target = targetTable;
  const needsIdentity = goal === "merge" || goal === "sync";
  const steps = needsIdentity
    ? ["Goal", "Dataset", "Upload", "Identity", "Review"]
    : ["Goal", "Dataset", "Upload", "Review"];
  const visibleStep = step;

  function resetDerived() {
    setStaged(undefined);
    setPreview(undefined);
    setSuccess(false);
  }

  function chooseGoal(nextGoal: Goal) {
    setGoal(nextGoal);
    setStep(0);
    setTargetTable("");
    setDatasetName("");
    setIdentity([]);
    resetDerived();
  }

  async function stage() {
    if (!file || !target) return;
    setLoading(true);
    setError(undefined);
    try {
      const result = await backend.stage_import({
        goal,
        target_table: target,
        uploaded_file: await filePayload(file),
      });
      setStaged(result);
      setPreview(undefined);
      setStep(needsIdentity ? 3 : 4);
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
      setPreview(
        await backend.preview_import({
          import_id: staged.import_id,
          identity_fields: identity,
          update_policy: policy,
        }),
      );
      setStep(4);
    } catch (reason) {
      setError(message(reason));
    } finally {
      setLoading(false);
    }
  }

  async function confirm() {
    if (!staged || !preview) return;
    setLoading(true);
    setError(undefined);
    try {
      await backend.apply_import({ import_id: staged.import_id });
      setSuccess(true);
    } catch (reason) {
      setError(message(reason));
    } finally {
      setLoading(false);
    }
  }

  const canMoveDataset = goal === "create" ? nameAvailable === true : Boolean(targetTable);

  return (
    <main className="app-shell">
      <header>
        <p className="eyebrow">Guardian Connector</p>
        <h1>Dataset importer</h1>
        <p className="lede">A staging POC for deliberate dataset changes.</p>
      </header>

      <ol className="progress" aria-label="Import progress">
        {steps.map((label, index) => (
          <li className={visibleStep >= index ? "active" : ""} key={label}>
            <span>{index + 1}</span>
            {label}
          </li>
        ))}
      </ol>

      {error && <div className="notice error" role="alert">{error}</div>}

      {visibleStep === 0 && (
        <section>
          <h2>What's your goal?</h2>
          <div className="goal-grid">
            {goals.map((option) => (
              <button
                className={`goal-card ${goal === option.id ? "selected" : ""}`}
                key={option.id}
                onClick={() => chooseGoal(option.id)}
                type="button"
              >
                <strong>{option.title}</strong>
                <span>{option.description}</span>
              </button>
            ))}
          </div>
        </section>
      )}

      {visibleStep === 1 && (
        <section>
          <h2>{goal === "create" ? "Name your new dataset" : "Select target dataset"}</h2>
          {goal === "create" ? (
            <label className="field">
              Dataset name
              <input
                autoFocus
                onChange={(event) => {
                  setDatasetName(event.target.value);
                  resetDerived();
                }}
                placeholder="Bird observations"
                value={datasetName}
              />
              {datasetName && nameAvailable === undefined && <small>Checking availability...</small>}
              {nameAvailable === true && <small className="valid">This name is available.</small>}
              {nameAvailable === false && <small className="invalid">A dataset with this name already exists.</small>}
            </label>
          ) : (
            <label className="field">
              Dataset
              <select onChange={(event) => { setTargetTable(event.target.value); resetDerived(); }} value={targetTable}>
                <option value="">Choose a dataset</option>
                {datasets.map((dataset) => <option key={dataset} value={dataset}>{dataset}</option>)}
              </select>
            </label>
          )}
        </section>
      )}

      {visibleStep === 2 && (
        <section>
          <h2>Upload your data</h2>
          <label className="dropzone">
            <input accept=".csv,.geojson,.json" onChange={(event) => { setFile(event.target.files?.[0]); resetDerived(); }} type="file" />
            <strong>{file ? file.name : "Drop a file here, or click to browse"}</strong>
            <span>CSV and GeoJSON only in this POC. Maximum source size: 25 MiB.</span>
          </label>
        </section>
      )}

      {visibleStep === 3 && needsIdentity && staged && (
        <section>
          <h2>Choose record identity</h2>
          <p className="lede">Select one to three fields that together identify a record.</p>
          <div className="identity-fields">
            {[0, 1, 2].map((position) => (
              <select
                aria-label={`Identity field ${position + 1}`}
                key={position}
                onChange={(event) => {
                  const next = [...identity];
                  if (event.target.value) next[position] = event.target.value;
                  else next.splice(position, 1);
                  setIdentity([...new Set(next)]);
                  setPreview(undefined);
                }}
                value={identity[position] ?? ""}
              >
                <option value="">{position === 0 ? "Choose a field" : "Add another field (optional)"}</option>
                {staged.fields.filter((field) => !identity.includes(field) || identity[position] === field).map((field) => <option key={field} value={field}>{field}</option>)}
              </select>
            ))}
          </div>
          <fieldset>
            <legend>Update policy</legend>
            <label><input checked={policy === "imported"} name="policy" onChange={() => { setPolicy("imported"); setPreview(undefined); }} type="radio" /> Imported records win, including empty values.</label>
            <label><input checked={policy === "existing"} name="policy" onChange={() => { setPolicy("existing"); setPreview(undefined); }} type="radio" /> Existing records win; matching rows stay unchanged.</label>
          </fieldset>
        </section>
      )}

      {visibleStep === 4 && (
        <section>
          <h2>Review import</h2>
          {!preview && <p className="lede">Generate a preview before writing to the dataset.</p>}
          {preview && (
            <div className="review-grid">
              <Metric label="Rows deleted" tone="danger" value={preview.deleted} />
              <Metric label="Rows updated" tone="blue" value={preview.updated} />
              <Metric label="Rows added" tone="gold" value={preview.added} />
              <Metric label="Columns added" tone="gold" value={preview.columns_added} />
              <Metric label="Unchanged records" tone="neutral" value={preview.unchanged} />
              <Metric label="Final record count" tone="neutral" value={preview.final_count} />
            </div>
          )}
          {success && <div className="notice success">Import applied successfully.</div>}
        </section>
      )}

      <footer className="actions">
        <button disabled={visibleStep === 0 || loading || success} onClick={() => { setStep(!needsIdentity && step === 4 ? 2 : Math.max(0, step - 1)); setError(undefined); }} type="button">Back</button>
        {visibleStep === 0 && <button className="primary" onClick={() => setStep(1)} type="button">Next</button>}
        {visibleStep === 1 && <button className="primary" disabled={!canMoveDataset} onClick={() => setStep(2)} type="button">Next</button>}
        {visibleStep === 2 && <button className="primary" disabled={!file || !target || loading} onClick={stage} type="button">{loading ? "Staging..." : "Stage upload"}</button>}
        {visibleStep === 3 && <button className="primary" disabled={identity.length === 0 || loading} onClick={makePreview} type="button">{loading ? "Calculating..." : "Preview changes"}</button>}
        {visibleStep === 4 && !preview && <button className="primary" disabled={loading} onClick={makePreview} type="button">{loading ? "Calculating..." : "Preview changes"}</button>}
        {visibleStep === 4 && preview && <button className="primary" disabled={loading || success} onClick={confirm} type="button">{loading ? "Importing..." : "Confirm import"}</button>}
      </footer>
    </main>
  );
}

function Metric({ label, tone, value }: { label: string; tone: string; value: number }) {
  return <article className={`metric ${tone}`}><strong>{value}</strong><span>{label}</span></article>;
}
