import { opendir, realpath } from "node:fs/promises";
import path from "node:path";

/** Resolve one Explorer media URL without changing the view or storage. */
export async function main(
  share_url: string,
  filebrowser_origin: string,
  username: string,
  password: string,
  storage_prefix = "datalake",
  mount_path = "",
) {
  let origin: URL;
  let shareUrl: URL;
  try {
    origin = new URL(filebrowser_origin);
    shareUrl = new URL(share_url);
  } catch {
    throw new Error("Invalid Filebrowser origin or share URL");
  }
  if (
    origin.protocol !== "https:" ||
    origin.username ||
    origin.password ||
    origin.pathname !== "/" ||
    origin.search ||
    origin.hash ||
    shareUrl.origin !== origin.origin ||
    shareUrl.username ||
    shareUrl.password ||
    shareUrl.search ||
    shareUrl.hash
  ) {
    throw new Error(
      "Share URL must use the configured HTTPS Filebrowser origin",
    );
  }
  const match = shareUrl.pathname.match(
    /^\/(?:share|api\/public\/dl)\/([A-Za-z0-9_-]+)\/?$/,
  );
  if (!match)
    throw new Error("Expected a Filebrowser share URL or download base URL");

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
      throw new Error(
        "Unable to reach Filebrowser; check the origin, network, and redirects",
      );
    }
    if (!response.ok)
      throw new Error(`Filebrowser API returned HTTP ${response.status}`);
    return response;
  };
  const token = (
    await (await api("/api/login", undefined, { username, password })).text()
  ).trim();
  if (!token) throw new Error("Filebrowser returned an empty login token");
  const shares: unknown = await (await api("/api/shares", token)).json();
  if (!Array.isArray(shares))
    throw new Error("Filebrowser returned an invalid share list");
  const matching = shares.filter((share) => share?.hash === match[1]);
  if (matching.length !== 1)
    throw new Error(
      "Share not found uniquely; check the URL and account permissions",
    );
  const share = matching[0];
  if (!Number.isSafeInteger(share.userID) || share.userID < 1)
    throw new Error("Invalid share owner ID");
  const owner: unknown = await (
    await api(`/api/users/${share.userID}`, token)
  ).json();
  if (
    !owner ||
    typeof owner !== "object" ||
    !("scope" in owner) ||
    typeof owner.scope !== "string" ||
    !owner.scope
  ) {
    throw new Error(
      "Owner scope unavailable; use a Filebrowser administrator account",
    );
  }
  const clean = (value: unknown): string => {
    if (
      typeof value !== "string" ||
      value.includes("\\") ||
      value.includes("\0") ||
      value.split("/").some((part) => part === "." || part === "..")
    ) {
      throw new Error("Invalid storage path");
    }
    return value.split("/").filter(Boolean).join("/");
  };
  const scope = owner.scope === "." ? "" : clean(owner.scope);
  const sharePath = clean(share.path);
  const smbPath = path.posix.join(clean(storage_prefix), scope, sharePath);
  const result: {
    owner_id: number;
    owner_scope: string;
    share_path: string;
    candidate_smb_relative_path: string;
    verified: boolean;
    mount_check?: { directory: string; entries: string[]; truncated: boolean };
  } = {
    owner_id: share.userID,
    owner_scope: `/${scope}`,
    share_path: `/${sharePath}`,
    candidate_smb_relative_path: smbPath,
    verified: false,
  };

  if (mount_path) {
    if (!path.isAbsolute(mount_path))
      throw new Error("mount_path must be absolute");
    const root = await realpath(mount_path);
    const directory = await realpath(path.join(root, smbPath));
    const relative = path.relative(root, directory);
    if (
      relative === ".." ||
      relative.startsWith(`..${path.sep}`) ||
      path.isAbsolute(relative)
    ) {
      throw new Error("Resolved directory is outside the mounted storage root");
    }
    const entries: string[] = [];
    let truncated = false;
    const dir = await opendir(directory);
    for await (const entry of dir) {
      if (entries.length === 20) {
        truncated = true;
        break;
      }
      entries.push(entry.name);
    }
    result.mount_check = { directory, entries, truncated };
  }
  return result;
}
