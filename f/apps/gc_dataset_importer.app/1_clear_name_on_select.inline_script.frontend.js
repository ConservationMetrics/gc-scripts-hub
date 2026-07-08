if (selectExistingDataset.result) {
  state.existingDatasetSelection = selectExistingDataset.result;
  state.newNameDefault = { "Dataset name": "" };
}
