if (selectDatasetTable.result) {
  state.dataset = selectDatasetTable.result
} else {
  state.dataset = null
}

if (selectLocalContextsTable.result) {
  state.localContextsTable = selectLocalContextsTable.result
} else {
  state.localContextsTable = null
}
