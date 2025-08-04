if (state.finalizeSuccess === true) {
  if (state.dataSource) {
    return `✅ File successfully transformed and written to the data warehouse!`
  } else {
    return "✅ File successfully written to the data warehouse!"
  }
} else {
  return ""
}