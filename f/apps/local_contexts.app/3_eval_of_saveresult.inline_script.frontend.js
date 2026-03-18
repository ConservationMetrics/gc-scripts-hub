if (state.finalizeSuccess) {
  return `✅ Local Contexts Labels have been applied to your dataset! You can find the Label mapping in ${state.datasetName}__lc_labels.`;
} else if (!state.finalizeSuccess && state.finalizeErrorMessage) {
  return `❌ ${state.finalizeErrorMessage}`;
} else {
  return "";
}
