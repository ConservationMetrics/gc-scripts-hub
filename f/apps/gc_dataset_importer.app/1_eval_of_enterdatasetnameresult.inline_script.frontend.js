switch (state.datasetAvailable) {
  case true:
    return `✅ New dataset! The database table "${state.validSqlname}" will be created.`;
  case false:
    return `⚠️ Dataset "${state.validSqlname}" already exists. New data will be appended to the existing table.`;
  default:
    return "";
}