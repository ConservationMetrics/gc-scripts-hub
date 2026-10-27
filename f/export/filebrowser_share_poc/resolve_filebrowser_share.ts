import { opendir, realpath } from "node:fs/promises";
import path from "node:path";
import pg from "pg";

type Postgresql = {
  host: string;
  port: number;
  user: string;
  password: string;
  dbname: string;
  sslmode?: string;
  root_certificate_pem?: string;
};
type View = {
  view_id: number;
  view_name: string;
  view_type: string | null;
  primary_dataset: string;
  view_config: string;
};
type Share = { hash: string; userID: number; path: string; expire?: number };
const fields = [
  "MEDIA_BASE_PATH",
  "MEDIA_BASE_PATH_ALERTS",
  "MEDIA_BASE_PATH_ICONS",
] as const;

function clean(value: unknown): string {
  if (
    typeof value !== "string" ||
    value.includes("\\") ||
    value.includes("\0") ||
    value.split("/").some((part) => part === "." || part === "..")
  ) {
    throw new Error("Invalid storage path");
  }
  return value.split("/").filter(Boolean).join("/");
}

/** Generate a candidate mapping from a snapshot of Explorer views. */
export async function reportViews(
  views: View[],
  originInput: string,
  username: string,
  password: string,
  storagePrefix: string,
  mountPath: string,
) {
  const origin = new URL(originInput);
  if (
    origin.protocol !== "https:" ||
    origin.username ||
    origin.password ||
    origin.pathname !== "/" ||
    origin.search ||
    origin.hash
  )
    throw new Error(
      "Filebrowser origin must be an HTTPS origin without a path",
    );
  const prefix = clean(storagePrefix);
  if (mountPath && !path.isAbsolute(mountPath))
    throw new Error("mount_path must be absolute");
  const result: Array<Record<string, unknown>> = [];
  const pending: Array<{ row: Record<string, unknown>; hash: string }> = [];
  for (const view of views) {
    const identity = {
      view_id: view.view_id,
      view_name: view.view_name,
      view_type: view.view_type,
      primary_dataset: view.primary_dataset,
    };
    let config: Record<string, unknown>;
    try {
      const parsed: unknown = JSON.parse(view.view_config);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
        throw new Error();
      config = parsed as Record<string, unknown>;
    } catch {
      result.push({ ...identity, status: "invalid_view_config" });
      continue;
    }
    let populated = false;
    for (const field of fields) {
      const value = config[field];
      if (value === undefined || value === null || value === "") continue;
      populated = true;
      const row: Record<string, unknown> = { ...identity, field };
      result.push(row);
      if (typeof value !== "string") {
        row.status = "invalid_media_value";
        continue;
      }
      let url: URL;
      try {
        url = new URL(value);
      } catch {
        row.status = "unsupported_media_value";
        continue;
      }
      if (url.origin !== origin.origin) {
        row.status = "other_origin";
        row.origin = url.origin;
        continue;
      }
      const match = url.pathname.match(
        /^\/(?:share|api\/public\/dl)\/([A-Za-z0-9_-]+)\/?$/,
      );
      if (!match || url.username || url.password || url.search || url.hash) {
        row.status = "unsupported_media_value";
        continue;
      }
      pending.push({ row, hash: match[1] });
    }
    if (!populated) result.push({ ...identity, status: "no_media_configured" });
  }
  if (!pending.length)
    return { dry_run: true, views_scanned: views.length, results: result };

  const api = async (apiPath: string, token?: string, data?: object) => {
    let response: Response;
    try {
      response = await fetch(`${origin.origin}${apiPath}`, {
        method: data ? "POST" : "GET",
        headers: {
          ...(token ? { "X-Auth": token } : {}),
          ...(data ? { "Content-Type": "application/json" } : {}),
        },
        ...(data ? { body: JSON.stringify(data) } : {}),
        redirect: "error",
        signal: AbortSignal.timeout(15000),
      });
    } catch {
      throw new Error("Filebrowser network or redirect error");
    }
    if (!response.ok)
      throw new Error(`Filebrowser API returned HTTP ${response.status}`);
    return response;
  };
  let token: string;
  let shares: Share[];
  try {
    token = (
      await (await api("/api/login", undefined, { username, password })).text()
    ).trim();
    if (!token) throw new Error("Filebrowser returned an empty login token");
    const raw: unknown = await (await api("/api/shares", token)).json();
    if (!Array.isArray(raw))
      throw new Error("Filebrowser returned an invalid share list");
    shares = raw;
  } catch (error) {
    for (const { row } of pending) {
      row.status = "filebrowser_unavailable";
      row.reason = error instanceof Error ? error.message : "Filebrowser error";
    }
    return { dry_run: true, views_scanned: views.length, results: result };
  }
  const ownerCache = new Map<number, Promise<string>>();
  const candidateCache = new Map<string, Promise<Record<string, unknown>>>();
  const resolve = async (hash: string): Promise<Record<string, unknown>> => {
    const matches = shares.filter((item) => item?.hash === hash);
    if (matches.length !== 1) return { status: "share_not_found_or_ambiguous" };
    const share = matches[0];
    if (!Number.isSafeInteger(share.userID) || share.userID < 1)
      throw new Error("Invalid share owner ID");
    if (!ownerCache.has(share.userID))
      ownerCache.set(
        share.userID,
        (async () => {
          const owner: unknown = await (
            await api(`/api/users/${share.userID}`, token)
          ).json();
          if (
            !owner ||
            typeof owner !== "object" ||
            !("scope" in owner) ||
            typeof owner.scope !== "string" ||
            !owner.scope
          )
            throw new Error(
              "Owner scope unavailable; use an administrator account",
            );
          return owner.scope === "." ? "" : clean(owner.scope);
        })(),
      );
    const scope = await ownerCache.get(share.userID)!;
    const sharePath = clean(share.path);
    const smbPath = path.posix.join(prefix, scope, sharePath);
    const candidate: Record<string, unknown> = {
      status: "resolved_candidate",
      owner_id: share.userID,
      owner_scope: `/${scope}`,
      share_path: `/${sharePath}`,
      candidate_smb_relative_path: smbPath,
      share_expire: share.expire ?? null,
      verified: false,
    };
    if (mountPath) {
      try {
        const root = await realpath(mountPath);
        const directory = await realpath(path.join(root, smbPath));
        const relative = path.relative(root, directory);
        if (
          relative === ".." ||
          relative.startsWith(`..${path.sep}`) ||
          path.isAbsolute(relative)
        )
          throw new Error("Path escapes mount");
        const entries: string[] = [];
        let truncated = false;
        for await (const entry of await opendir(directory)) {
          if (entries.length === 20) {
            truncated = true;
            break;
          }
          entries.push(entry.name);
        }
        candidate.mount_check = {
          status: "directory_exists",
          directory,
          entries,
          truncated,
        };
      } catch {
        candidate.mount_check = { status: "unavailable_or_outside_mount" };
      }
    }
    return candidate;
  };
  for (const { row, hash } of pending) {
    if (!candidateCache.has(hash)) candidateCache.set(hash, resolve(hash));
    try {
      Object.assign(row, await candidateCache.get(hash)!);
    } catch (error) {
      row.status = "resolution_error";
      row.reason = error instanceof Error ? error.message : "Resolution failed";
    }
  }
  return { dry_run: true, views_scanned: views.length, results: result };
}

/** Read the configuration database in a read-only transaction. */
export async function main(
  db: Postgresql,
  filebrowser_origin: string,
  username: string,
  password: string,
  expected_config_database = "guardianconnector",
  storage_prefix = "datalake",
  mount_path = "",
) {
  if (
    !db ||
    db.dbname !== expected_config_database ||
    !expected_config_database
  )
    throw new Error(
      "PostgreSQL resource database must match expected_config_database",
    );
  const mode = db.sslmode || "disable";
  if (!["disable", "require", "verify-ca", "verify-full"].includes(mode))
    throw new Error("Unsupported Postgres sslmode");
  const ssl =
    mode === "disable"
      ? false
      : {
          rejectUnauthorized: mode !== "require",
          ...(db.root_certificate_pem ? { ca: db.root_certificate_pem } : {}),
        };
  const client = new pg.Client({
    host: db.host,
    port: db.port,
    user: db.user,
    password: db.password,
    database: db.dbname,
    ssl,
    connectionTimeoutMillis: 15000,
  });
  let views: View[];
  try {
    await client.connect();
    await client.query("BEGIN READ ONLY");
    const response = await client.query(
      "SELECT view_id, view_name, view_type, primary_dataset, view_config FROM public.views ORDER BY view_id",
    );
    views = response.rows;
  } catch {
    throw new Error(
      "Unable to read Explorer views; verify database access and schema",
    );
  } finally {
    await client.end().catch(() => {});
  }
  return reportViews(
    views,
    filebrowser_origin,
    username,
    password,
    storage_prefix,
    mount_path,
  );
}
