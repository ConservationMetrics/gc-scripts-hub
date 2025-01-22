if (state.uploadSuccess && state.uploadSuccess === true) {
  return "File uploaded successfully!"
} else if (state.uploadProcessing && state.uploadProcessing === true) {
  return "Processing..."
} else {
  return ""
}