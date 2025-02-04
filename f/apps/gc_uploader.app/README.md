# GC Uploader (Locus Maps)

This app is designed to take a Locus Map export file (CSV/KML/GPX or ZIP with attachments), and trigger the [`/f/connectors/locusmap/`](/f/connectors/locusmap/README.md) script with the temporary file path after successful upload.

The app uses state management, inline scripts, and three UI components to accomplish the desired workflow:

* `selectFile` (File Input) allows the user to select a Locus Map export file. Only CSV, KML, GPX, and ZIP mimetypes are supported.
* `uploadFile` (Button) when pressed, will trigger the `save_data_to_disk` inline script to run. This component is deactivated until the user selects a file (via `uploadButtonEnabled` state).
* which will upload the file to a temporary location on the disk.
* Once `save_data_to_disk` is complete, it will trigger the Locus Map connector script to run.
* Several frontline scripts are triggered along the way to update state variables such as `uploadProcessing` and `uploadSuccess`.
* `resultMessage` (Text) will show a status message of "Processing...", "File uploaded successfully!", or nothing depending on the value of the state variables.

## TODO

* Allowing the user to change the name of their database table; for now, it is taking the filename point blank.
* Styling and design of the app. The look-and-feel is out of the box from Windmill apps.
* The "Save data to disk" script to handle the file upload was added to the app as an inline script. We may want to make it a standalone script so it can be used by other Windmill apps.
* Error handling. If something goes wrong in the Locus Map connector script, `resultMessage` is not updated with error messaging, but the Windmill app UI does show an error message modal out-of-the-box.