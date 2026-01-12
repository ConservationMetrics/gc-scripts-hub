const totalColumns = state.newColumns + (state.dataSource ? 1 : 0)

return `${state.newRows} new rows, ${state.updatedRows} updated rows, ${totalColumns} columns`