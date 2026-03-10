if (state.newRows === 0 && state.updatedRows === 0) {
  return "No rows to update!"
} else if (state.finalizeSuccess) {
  return "✅ Dataset successfully written to the data warehouse! Navigate to a Guardian Connector tool like Explorer to set up a view for your dataset."
} else if (!state.finalizeSuccess && state.finalizeErrorMessage) { 
  return `❌ ${state.finalizeErrorMessage}`
} else {
  return ""
}