const { currentStepIndex, lastAction } = formStepper;

// Step 1: Dataset name must be valid
if (currentStepIndex === 0 && !state.validSqlName) {
  throw new Error("Please enter a valid dataset name to proceed.");
}

// Step 2: File must be uploaded
if (currentStepIndex === 1 && lastAction === "next" && !state.uploadSuccess) {
  throw new Error("Please upload your file to proceed.");
}

// Step 4: Can't reuse same session
if (currentStepIndex === 3 && state.finalizeSuccess) {
  throw new Error("Please refresh the page to upload another file.");
}
