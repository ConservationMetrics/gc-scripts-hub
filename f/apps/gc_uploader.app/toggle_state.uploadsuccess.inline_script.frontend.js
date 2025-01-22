if (selectFile.result && uploadFile.result && !uploadFile.result.error) {
  state.uploadSuccess = true;
  state.uploadProcessing = false;
}