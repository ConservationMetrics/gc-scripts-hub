if (dataSourceToggle.result) {
  state.dataSource = dataSources.result;
} else if (!dataSourceToggle.result) {
  state.dataSource = undefined;
}