const newName = enterNewDatasetName.values?.["Dataset name"]?.trim();

if (selectExistingDataset.result && !newName) {
  state.existingDatasetSelection = selectExistingDataset.result;
} else if (newName) {
  state.existingDatasetSelection = null;
} else {
  state.existingDatasetSelection = null;
}
