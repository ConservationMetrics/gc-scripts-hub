if (state.finalizeSuccess === true) {
  if (state.dataSource) {
    return `✅ File successfully transformed and written to the data warehouse! Reload the page to upload another dataset, or navigate to a Guardian Connector tool like Explorer to set up a view for your dataset.`
  } else {
    return "✅ File successfully written to the data warehouse! Reload the page to upload another dataset, or navigate to a Guardian Connector tool like Explorer to set up a view for your dataset."
  }
} else {
  return ""
}