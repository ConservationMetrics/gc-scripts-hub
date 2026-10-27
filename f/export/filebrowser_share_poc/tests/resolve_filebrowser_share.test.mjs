import assert from "node:assert/strict";
import { test, mock } from "node:test";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pg from "pg";
import { main, reportViews } from "../resolve_filebrowser_share.ts";

const origin = "https://files.example.com";
const view = (id, config) => ({
  view_id: id,
  view_name: `view ${id}`,
  view_type: "map",
  primary_dataset: "dataset",
  view_config: JSON.stringify(config),
});
const url = `${origin}/api/public/dl/hash`;
function mockFilebrowser(scope = "/project") {
  const calls = [];
  mock.method(globalThis, "fetch", async (url, options) => {
    calls.push({ url, options });
    if (url.endsWith("/api/login")) return new Response("jwt");
    if (url.endsWith("/api/shares"))
      return Response.json([
        { hash: "hash", userID: 3, path: "/media", expire: 0 },
      ]);
    if (url.endsWith("/api/users/3")) return Response.json({ scope });
    throw new Error("unexpected URL");
  });
  return calls;
}

test("reports all views and media fields, caches lookups, and checks the mount", async () => {
  const dir = await mkdtemp(join(tmpdir(), "fb-poc-"));
  try {
    await mkdir(join(dir, "datalake/project/media"), { recursive: true });
    await writeFile(join(dir, "datalake/project/media/photo.jpg"), "photo");
    const calls = mockFilebrowser();
    const report = await reportViews(
      [
        view(1, {
          MEDIA_BASE_PATH: url,
          MEDIA_BASE_PATH_ICONS: url,
          MEDIA_BASE_PATH_ALERTS: "https://another.example/share/other",
        }),
        view(2, { MEDIA_BASE_PATH: `${origin}/share/missing` }),
        view(3, {}),
        { ...view(4, {}), view_config: "{" },
      ],
      origin,
      "admin",
      "secret",
      "datalake",
      dir,
    );
    assert.equal(report.views_scanned, 4);
    assert.deepEqual(
      report.results.map((x) => x.status),
      [
        "resolved_candidate",
        "other_origin",
        "resolved_candidate",
        "share_not_found_or_ambiguous",
        "no_media_configured",
        "invalid_view_config",
      ],
    );
    assert.equal(
      report.results[0].candidate_smb_relative_path,
      "datalake/project/media",
    );
    assert.deepEqual(report.results[0].mount_check.entries, ["photo.jpg"]);
    assert.equal(report.results[0].verified, false);
    assert.equal(calls.length, 3);
    assert.equal(calls[1].options.headers["X-Auth"], "jwt");
    assert.equal(calls[0].options.redirect, "error");
    assert.ok(!JSON.stringify(report).includes("secret"));
  } finally {
    mock.restoreAll();
    await rm(dir, { recursive: true, force: true });
  }
});

test("rejects untrusted URLs before sending credentials", async () => {
  const calls = mockFilebrowser();
  try {
    const report = await reportViews(
      [view(1, { MEDIA_BASE_PATH: "https://attacker.example/share/hash" })],
      origin,
      "admin",
      "secret",
      "datalake",
      "",
    );
    assert.equal(report.results[0].status, "other_origin");
    assert.equal(calls.length, 0);
  } finally {
    mock.restoreAll();
  }
});

test("reports unsafe scope and missing mount as separate cases", async () => {
  try {
    mockFilebrowser("/../escape");
    const bad = await reportViews(
      [view(1, { MEDIA_BASE_PATH: url })],
      origin,
      "admin",
      "secret",
      "datalake",
      "",
    );
    assert.equal(bad.results[0].status, "resolution_error");
    mock.restoreAll();
    mockFilebrowser(".");
    const missing = await reportViews(
      [view(1, { MEDIA_BASE_PATH: url })],
      origin,
      "admin",
      "secret",
      "datalake",
      "/missing-mount",
    );
    assert.equal(
      missing.results[0].candidate_smb_relative_path,
      "datalake/media",
    );
    assert.equal(
      missing.results[0].mount_check.status,
      "unavailable_or_outside_mount",
    );
  } finally {
    mock.restoreAll();
  }
});

test("uses a read-only transaction and checks the database name", async () => {
  const commands = [];
  mock.method(pg.Client.prototype, "connect", async () => {});
  mock.method(pg.Client.prototype, "query", async (sql) => {
    commands.push(sql);
    return { rows: [view(1, {})] };
  });
  mock.method(pg.Client.prototype, "end", async () => {});
  try {
    const db = {
      dbname: "guardianconnector",
      host: "db",
      port: 5432,
      user: "user",
      password: "password",
    };
    const report = await main(db, origin, "admin", "secret");
    assert.equal(report.views_scanned, 1);
    assert.match(commands[0], /BEGIN READ ONLY/);
    assert.match(commands[1], /SELECT .* FROM public.views/);
    await assert.rejects(
      main({ ...db, dbname: "warehouse" }, origin, "admin", "secret"),
      /database must match/,
    );
  } finally {
    mock.restoreAll();
  }
});

test("records a Filebrowser login failure for each affected field", async () => {
  mock.method(
    globalThis,
    "fetch",
    async () => new Response("denied", { status: 403 }),
  );
  try {
    const report = await reportViews(
      [view(1, { MEDIA_BASE_PATH: url, MEDIA_BASE_PATH_ICONS: url })],
      origin,
      "admin",
      "secret",
      "datalake",
      "",
    );
    assert.deepEqual(
      report.results.map((row) => row.status),
      ["filebrowser_unavailable", "filebrowser_unavailable"],
    );
    assert.equal(report.results[0].reason, "Filebrowser API returned HTTP 403");
  } finally {
    mock.restoreAll();
  }
});
