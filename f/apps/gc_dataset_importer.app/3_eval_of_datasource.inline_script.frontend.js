const converting = state.longitudeCol && state.latitudeCol;

if (!dataSourceToggle.result || !state.dataSource) {
  return converting
    ? "None selected, but converting to GeoJSON"
    : "None selected";
}

return converting
  ? `${state.dataSource}, and converting to GeoJSON`
  : state.dataSource;