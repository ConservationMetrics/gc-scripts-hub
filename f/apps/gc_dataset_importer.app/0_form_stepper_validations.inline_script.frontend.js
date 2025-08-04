// Step 1 validation
if (formStepper.currentStepIndex === 0 && !state.datasetAvailable) {
  throw new Error("Please enter a valid dataset name to proceed.");
}

// Step 2 validation
if (formStepper.currentStepIndex === 1 && formStepper.lastAction === "next" && !state.uploadSuccess ) {
  throw new Error("Please upload your file to proceed.");
}

// Step 4 validation
if (formStepper.currentStepIndex === 3 && state.finalizeSuccess === true) {
  throw new Error("Please refresh the page to upload another file.")
}