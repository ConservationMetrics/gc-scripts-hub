const { currentStepIndex } = formStepper;

// Step 1: Dataset must be selected
if (currentStepIndex === 0) {
  if (!state.datasetName) {
    throw new Error("Please select a dataset to proceed.");
  }
  if (!state.localContextsTable) {
    throw new Error("Please select a Local Contexts label-set to proceed.");
  }
}

// Step 2: At least one label must be selected AND at least one change made
if (currentStepIndex === 1) {
  const selectedLabels = Array.isArray(state.labelsToApply) ? state.labelsToApply : [];
  if (selectedLabels.length < 1) {
    throw new Error("Please select at least one Local Contexts label.");
  }

  const alreadyApplied = [
    ...(Array.isArray(state.tkLabelsAlreadyApplied) ? state.tkLabelsAlreadyApplied : []),
    ...(Array.isArray(state.bcLabelsAlreadyApplied) ? state.bcLabelsAlreadyApplied : []),
  ];

  const selectedSet = new Set(selectedLabels);
  const appliedSet = new Set(alreadyApplied);

  const hasNewLabel = selectedLabels.some((label) => !appliedSet.has(label));
  const hasRemovedLabel = alreadyApplied.some((label) => !selectedSet.has(label));

  if (!hasNewLabel && !hasRemovedLabel) {
    throw new Error("Please make at least one label change (add or remove) before continuing.");
  }
}

// Step 3: Can't reuse same session
if (currentStepIndex === 2 && state.finalizeSuccess) {
  throw new Error("Please refresh the page to apply labels to another dataset.");
}