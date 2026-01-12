if (state.uploadSuccess && !state.tableExists) {
  return "✅ File successfully uploaded to temporary storage! Please proceed to the next step to finish writing the data to the warehouse."
} else if (state.uploadSuccess && state.tableExists && state.newRows && state.updatedRows && state.newColumns) {
  return `✅ File successfully uploaded to temporary storage! ${state.newRows} new rows will be created, ${state.updatedRows} rows will be updated, and ${state.newColumns} new columns will be created.`
} else if (!state.uploadSuccess && state.uploadErrorMessage) { 
  return `❌ ${state.uploadErrorMessage}`
} else {
  return ""
}