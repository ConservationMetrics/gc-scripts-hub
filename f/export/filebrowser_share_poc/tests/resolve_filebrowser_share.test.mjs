import assert from "node:assert/strict";
import { test, mock } from "node:test";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { main } from "../resolve_filebrowser_share.ts";

const origin = "https://files.example.com";
const url = `${origin}/api/public/dl/hash`;

function mockFilebrowser(scope = "/project", sharePath = "/media") {
  const calls = [];
  mock.method(globalThis, "fetch", async (url, options) => {
    calls.push({ url, options });
    if (url.endsWith("/api/login")) return new Response("jwt");
    if (url.endsWith("/api/shares"))
      return Response.json([{ hash: "hash", userID: 3, path: sharePath }]);
    if (url.endsWith("/api/users/3")) return Response.json({ scope });
    throw new Error("unexpected URL");
  });
  return calls;
}

test("resolves owner scope and lists the candidate directory without exposing credentials", async () => {
  const dir = await mkdtemp(join(tmpdir(), "fb-poc-"));
  try {
    await mkdir(join(dir, "datalake/project/media"), { recursive: true });
    await writeFile(join(dir, "datalake/project/media/photo.jpg"), "photo");
    const calls = mockFilebrowser();
    const result = await main(url, origin, "admin", "secret", "datalake", dir);
    assert.equal(result.candidate_smb_relative_path, "datalake/project/media");
    assert.deepEqual(result.mount_check.entries, ["photo.jpg"]);
    assert.equal(result.verified, false);
    assert.equal(calls[1].options.headers["X-Auth"], "jwt");
    assert.equal(calls[0].options.redirect, "error");
    assert.ok(!JSON.stringify(result).includes("secret"));
  } finally {
    mock.restoreAll();
    await rm(dir, { recursive: true, force: true });
  }
});

test("rejects untrusted origins before sending credentials", async () => {
  const calls = mockFilebrowser();
  try {
    await assert.rejects(
      main("https://attacker.example/share/hash", origin, "admin", "secret"),
    );
    assert.equal(calls.length, 0);
  } finally {
    mock.restoreAll();
  }
});

test("rejects invalid owner scopes and handles the root scope", async () => {
  try {
    mockFilebrowser("/../escape");
    await assert.rejects(
      main(url, origin, "admin", "secret"),
      /Invalid storage path/,
    );
    mock.restoreAll();
    mockFilebrowser(".");
    const result = await main(url, origin, "admin", "secret");
    assert.equal(result.candidate_smb_relative_path, "datalake/media");
  } finally {
    mock.restoreAll();
  }
});
