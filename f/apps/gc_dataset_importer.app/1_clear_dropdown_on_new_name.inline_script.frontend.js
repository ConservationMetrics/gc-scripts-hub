const newName = enterNewDatasetName.values?.["Dataset name"]?.trim();

if (newName) {
  state.existingDatasetSelection = null;
}

// Clear the previously resolved dataset immediately so the result message
// doesn't flash the stale (e.g. dropdown) value while the async DB check re-runs.
state.tableExists = null;
state.validSqlName = null;
state.datasetName = null;
