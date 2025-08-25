switch (state.datasetAvailable) {
  case true:
    return `✅ Dataset name is available! The database table name will be "${state.validSqlname}".`;
  case false:
    return "⚠️ Dataset name is already in usage.";
  default:
    return "";
}