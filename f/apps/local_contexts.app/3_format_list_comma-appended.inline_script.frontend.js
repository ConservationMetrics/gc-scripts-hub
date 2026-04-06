const allLabels = [
  ...(state.tkLabelsAvailable || []),
  ...(state.bcLabelsAvailable || [])
]

return (state.labelsToApply || [])
  .map(val => allLabels.find(l => l.value === val)?.label || val)
  .join(", ")