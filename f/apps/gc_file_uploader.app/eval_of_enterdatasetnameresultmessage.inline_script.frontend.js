switch (state.datasetAvailable) {
  case true:
    return "✅ Dataset name is available!";
  case false:
    return "⚠️ Dataset name is already in usage.";
  default:
    return "";
}