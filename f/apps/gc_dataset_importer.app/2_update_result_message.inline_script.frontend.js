if (state.uploadSuccess) {
  return "✅ File successfully uploaded to temporary storage! Please proceed to the next step to finish writing the data to the warehouse."
} else if (!state.uploadSuccess && state.uploadErrorMessage) { 
  return `❌ ${state.uploadErrorMessage}`
} else {
  return ""
}