const { currentStepIndex, lastAction } = formStepper;

// Step 1: Dataset name must be valid
if (currentStepIndex === 0 && !state.validSqlName) {
  throw new Error("Please enter a valid dataset name to proceed.");
}

// Step 2: File must be uploaded
if (currentStepIndex === 1 && lastAction === "next" && !state.uploadSuccess) {
  throw new Error("Please upload your file to proceed.");
}

// Step 3: Must have both longitude and latitude cols, or neither
if (currentStepIndex === 2 && !!longitudeCol.result !== !!latitudeCol.result) {
  throw new Error(
    "Please specify both a longitude and latitude column, or erase both.",
  );
  // TODO: consider validating a sample of the columns' values to ensure they are valid longitude and latitude
}

// Step 3: Set longitude and latitude columns
if (currentStepIndex === 2) {
  if (lonLatToggle.result && longitudeCol.result && latitudeCol.result) {
    state.longitudeCol = longitudeCol.result;
    state.latitudeCol = latitudeCol.result;
  } else {
    state.longitudeCol = null;
    state.latitudeCol = null;
  }
}

// Step 4: Can't reuse same session
if (currentStepIndex === 3 && state.finalizeSuccess) {
  throw new Error("Please refresh the page to upload another file.");
}
