const { currentStepIndex, lastAction } = formStepper;

// Step 1: Dataset must be selected
if (currentStepIndex === 0) {
  if (!state.dataset) {
    throw new Error("Please select a dataset to proceed.");
  }
  if (!state.localContextsTable) {
    throw new Error("Please select a Local Contexts label-set to proceed.");
  }
}