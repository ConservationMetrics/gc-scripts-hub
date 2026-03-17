const { currentStepIndex, lastAction } = formStepper;

// Step 1: Dataset must be selected
if (currentStepIndex === 0) {
  if (!state.datasetName) {
    throw new Error("Please select a dataset to proceed.");
  }
  if (!state.localContextsTable) {
    throw new Error("Please select a Local Contexts label-set to proceed.");
  }
  if (!state.tkLabels || !state.bcLabels) {
    throw new Error ("Please wait until Local Contexts labels have been loaded. Or if you have been waiting a while already, your Local Contexts label set might be empty.")
  }
}

// Step 2: At least one label must be applied
if (currentStepIndex === 1) {
  if (!state.labelsToApply || state.labelsToApply < 1) {
    throw new Error("Please select at least one Local Contexts label.");
  }
}