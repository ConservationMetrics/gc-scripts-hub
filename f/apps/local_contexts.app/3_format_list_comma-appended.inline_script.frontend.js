const allLabels = [
  ...(state.tkLabels || []),
  ...(state.bcLabels || [])
]

return (state.labelsToApply || [])
  .map(val => allLabels.find(l => l.value === val)?.label || val)
  .join(", ")