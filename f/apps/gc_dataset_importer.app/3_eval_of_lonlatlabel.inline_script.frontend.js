if (state.outputFormat === "csv") {
  return "📍 Does this data have latitude and longitude coordinates, and you want to convert it to GeoJSON? If so, select the columns:";
} else {
  return "🗺️ This data cannot be converted to GeoJSON because it is already spatial data.";
}