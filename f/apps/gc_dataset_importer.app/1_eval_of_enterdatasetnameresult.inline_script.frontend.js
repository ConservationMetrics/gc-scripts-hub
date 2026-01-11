switch (state.tableExists) {
  case false:
    return `✅ New dataset! The database table "${state.validSqlName}" will be created.`;
  case true:
    return `⚠️ Dataset "${state.validSqlName}" already exists. New data will be appended to the existing table.`;
  default:
    return "";
}