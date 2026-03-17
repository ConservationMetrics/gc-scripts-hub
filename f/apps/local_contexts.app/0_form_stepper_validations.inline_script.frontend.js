const { currentStepIndex, lastAction } = formStepper;

// Step 1: Dataset must be selected
if (currentStepIndex === 0 && !state.dataset) {
  throw new Error("Please select a dataset to proceed.");
}