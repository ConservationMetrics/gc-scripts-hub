if (state.uploadSuccess && !state.tableExists && (state.newRows !== null && state.newRows !== undefined)) {
  // New dataset - show row and column counts
  const parts = [];
  if (state.newRows > 0) parts.push(`${state.newRows} row${state.newRows !== 1 ? 's' : ''}`);
  if (state.newColumns > 0) parts.push(`${state.newColumns} column${state.newColumns !== 1 ? 's' : ''}`);
  
  if (parts.length > 0) {
    return `✅ File uploaded! New dataset will be created with ${parts.join(' and ')}.`;
  } else {
    return "✅ File successfully uploaded to temporary storage! Please proceed to the next step to finish writing the data to the warehouse.";
  }
} else if (state.uploadSuccess && !state.tableExists) {
  return "✅ File successfully uploaded to temporary storage! Please proceed to the next step to finish writing the data to the warehouse."
} else if (state.uploadSuccess && state.tableExists && (state.newRows !== null && state.newRows !== undefined)) {
  // Build message with non-null stats: separate additions from updates
  const additions = [];
  const updates = [];
  
  if (state.newRows > 0) additions.push(`${state.newRows} new row${state.newRows !== 1 ? 's' : ''}`);
  if (state.newColumns > 0) additions.push(`${state.newColumns} new column${state.newColumns !== 1 ? 's' : ''}`);
  if (state.updatedRows > 0) updates.push(`${state.updatedRows} row${state.updatedRows !== 1 ? 's' : ''} updated`);
  
  if (additions.length > 0 || updates.length > 0) {
    const messages = [];
    if (additions.length > 0) messages.push(`${additions.join(', ')} will be added`);
    if (updates.length > 0) messages.push(updates.join(', '));
    return `➕ Appending to existing dataset! ${messages.join('; ')}.`;
  } else {
    return `✅ File uploaded! No changes detected - all rows and columns already exist with the same values.`;
  }
} else if (!state.uploadSuccess && state.uploadErrorMessage) { 
  return `❌ ${state.uploadErrorMessage}`
} else {
  return ""
}