if (state.uploadSuccess && !state.tableExists) {
  return "✅ File successfully uploaded to temporary storage! Please proceed to the next step to finish writing the data to the warehouse."
} else if (state.uploadSuccess && state.tableExists) {
  return "TODO: Provide helpful append event message"
} else if (!state.uploadSuccess && state.uploadErrorMessage) { 
  return `❌ ${state.uploadErrorMessage}`
} else {
  return ""
}