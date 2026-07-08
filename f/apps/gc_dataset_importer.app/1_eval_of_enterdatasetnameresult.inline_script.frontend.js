const newName = enterNewDatasetName.values?.["Dataset name"]?.trim();

if (!newName) {
  return "";
}

// Only show a result once the async check has resolved for THIS typed name.
// Otherwise the message would flash the previously resolved (e.g. dropdown) value.
if (state.datasetName !== newName) {
  return "";
}

switch (state.tableExists) {
  case true:
    return `⚠️ A dataset named "${state.validSqlName}" already exists. New data will be appended to it.`;
  case false:
    return `✅ New dataset! The database table "${state.validSqlName}" will be created.`;
  default:
    return "";
}
