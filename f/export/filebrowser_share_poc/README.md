# Filebrowser media migration dry run

This read-only Windmill script inventories every Explorer view and reports candidate SMB-relative paths for `MEDIA_BASE_PATH`, `MEDIA_BASE_PATH_ALERTS`, and `MEDIA_BASE_PATH_ICONS`. It reads the Explorer configuration database in a read-only transaction, logs in to one Filebrowser instance, and resolves shares and owner scopes. It does not update the database or storage.

## Run on cmistaging

1. Obtain an administrator login for `https://files.cmistaging.guardianconnector.net` with share and download permissions. In the cmistaging Windmill workspace, create a **secret variable** for its password in a restricted user or folder path. Never put the password in source code, logs, or a normal run argument.
2. Select or create a Windmill **PostgreSQL resource** for Explorer's configuration database, typically `guardianconnector`. This is separate from the `warehouse` database. The account needs `SELECT` on `public.views`. The script supports Postgres `sslmode` values `disable`, `require`, `verify-ca`, and `verify-full`; supply the appropriate CA certificate in the resource if required. It uses `require` without certificate verification, consistent with Explorer's current configuration; use a verification mode if supported by the server.
3. Push this script to the staging workspace using your normal deployment workflow. To target only this script, run from this repository:

   ```sh
   wmill script push f/export/filebrowser_share_poc/resolve_filebrowser_share.ts --workspace <staging-workspace>
   ```

   Check the workspace name first: this updates the named script in that workspace. The script's `pg` dependency is pinned by its Windmill lockfile.

4. Open **POC: Dry-run Filebrowser media migration** in Windmill and enter:
   - `db`: the Explorer configuration Postgres resource from step 2.
   - `filebrowser_origin`: `https://files.cmistaging.guardianconnector.net`.
   - `username`: the Filebrowser admin username.
   - `password`: use the variable picker to select the secret from step 1. The argument must be a secret reference (`$var:<variable-path>`), not the literal password. The execution identity needs permission to read the secret.
   - `expected_config_database`: normally `guardianconnector`; the script refuses to connect if the resource's database name does not match.
   - `storage_prefix`: `datalake` only if the staging Filebrowser root is the `datalake` directory at the root of the SMB share.
   - `mount_path`: optionally `/persistent-storage`, if that is the SMB share root mounted in the worker executing the job. Leave blank to resolve without accessing storage.
5. Run the job and review its results. Each configured field has a status, including `resolved_candidate`, `other_origin`, `unsupported_media_value`, `share_not_found_or_ambiguous`, or an error. Views without media and malformed configs are also reported. Only the configured Filebrowser origin is queried; review other origins separately with their own credentials and storage mapping. The job does not return share URLs or hashes.
6. Check cmistaging's actual Filebrowser root, user scopes, and volume mappings. A resolved path is a **candidate** relative to the SMB share root, not a UNC address. If a mount is supplied, the job lists up to 20 immediate directory entries when accessible; this is a sanity check, not proof of the mapping. Compare representative attachment contents with Filebrowser downloads. `verified` remains false for every candidate. Expiration is reported from the share record and should be reviewed.
7. Remove the temporary secret after testing. Windmill retains job results, which include view names, storage paths, and optional directory entries; restrict access to the job and its history.

The Filebrowser account must be able to list all relevant shares and read their owners' scopes. A non-admin account may return an incomplete list. Filebrowser requests only use the specified HTTPS origin and reject redirects. If the worker cannot see the mount, run without `mount_path` first; worker mounts and filesystem sandboxing vary by deployment.

This is not an apply migration: Explorer currently expects HTTP media URLs. An eventual migration needs a destination configuration and an authorized media-serving endpoint before any URLs can be replaced.

## Local tests

Install the `pg` dependency (the Windmill lockfile records the version) and run with Node 22.6 or later:

```sh
node --experimental-strip-types --test f/export/filebrowser_share_poc/tests/resolve_filebrowser_share.test.mjs
```
