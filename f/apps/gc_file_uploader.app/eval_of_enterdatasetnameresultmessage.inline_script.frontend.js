if (submitDatasetName.result === false) {
  return "✅ Dataset name is available!"
} else if (submitDatasetName.result === true) {
  return "⚠️ Dataset name is already in usage."
} else {
  return ""
}