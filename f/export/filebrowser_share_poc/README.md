# Filebrowser share resolution proof of concept

This read-only Windmill script resolves one Filebrowser share URL configured in an Explorer view. It logs in to Filebrowser, finds the share and its owner's scope, and returns a candidate path relative to the SMB share. It does not read or change Explorer's database, change files, or migrate views.

## Run on cmistaging

1. Obtain a Filebrowser admin login with share and download permissions. In the cmistaging Windmill workspace, create a **secret variable** for its password in a restricted user or folder path. Only users who should use this credential should have access to that path. Never paste the password into source code, logs, or a normal run argument.
2. Push this script to the staging workspace using your normal Windmill deployment workflow. To target only this script, after selecting the correct CLI workspace, run from this repository:

   ```sh
   wmill script push f/export/filebrowser_share_poc/resolve_filebrowser_share.ts --workspace <staging-workspace>
   ```

   Check the workspace name before running; this updates the named script in that workspace. Do not use a full workspace sync just for this test.

3. Open **POC: Resolve Filebrowser share to storage path** in Windmill and enter:
   - `share_url`: the `MEDIA_BASE_PATH`, `MEDIA_BASE_PATH_ALERTS`, or `MEDIA_BASE_PATH_ICONS` value copied from a staging Explorer view. The job input contains this URL, which grants access to the share; restrict access to the run and its history.
   - `filebrowser_origin`: `https://files.cmistaging.guardianconnector.net`.
   - `username`: the Filebrowser admin username.
   - `password`: use the Windmill variable picker to select the secret created in step 1. The argument must be a secret reference (`$var:<variable-path>`), not the literal password. The worker resolves that reference at execution. Ensure the job's execution identity has access to the secret.
   - `storage_prefix`: `datalake` only if the staging Filebrowser root is the `datalake` directory at the root of the SMB share.
   - `mount_path`: optionally `/persistent-storage`, if that is the SMB share root mounted in the **worker that executes this job**. Leave blank to resolve without accessing storage.
4. Run the script and inspect its result. `candidate_smb_relative_path` is relative to the SMB share root, not a complete UNC address. If a mount is supplied, `mount_check` lists up to 20 immediate entries from the candidate directory. The list and path may contain sensitive names and are saved in job results. An existing directory is not proof that the mapping is correct: confirm the actual Filebrowser root, owner scope and mounts, then compare a known attachment against its Filebrowser download. `verified` remains false.
5. Remove the temporary secret after testing. Review Windmill job history and its access permissions according to your team's practices.

The Filebrowser API account must be able to list the share and read its owner's scope. A normal user may not be able to resolve another user's shares. The script accepts only HTTPS share URLs on the explicitly configured origin and refuses redirects. It does not support password-protected public download verification. If the worker cannot access the mount, run without `mount_path` first; worker mounts and filesystem sandboxing vary by deployment.

## Local tests

With Node 22.6 or later:

```sh
node --experimental-strip-types --test f/export/filebrowser_share_poc/tests/resolve_filebrowser_share.test.mjs
```
