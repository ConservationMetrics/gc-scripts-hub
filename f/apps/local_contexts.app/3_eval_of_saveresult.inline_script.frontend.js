if (state.finalizeSuccess) {
return `✅ Local Contexts labels have been applied to your dataset! You can find the label mapping in ${state.datasetName}__lc_labels.`
} else if (!state.finalizeSuccess && state.finalizeErrorMessage) { 
  return `❌ ${state.finalizeErrorMessage}`
} else {
  return ""
}