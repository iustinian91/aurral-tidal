// Aurral plugin: Tidal download source. Node built-ins + fetch only.
import { open, readFile, writeFile, rename, rm, mkdir, readdir, stat } from "node:fs/promises";
import { createReadStream, createWriteStream } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import { pipeline } from "node:stream/promises";
import { Readable, Transform } from "node:stream";
import { createHash, randomBytes } from "node:crypto";

const AUTH = "https://auth.tidal.com/v1/oauth2";
const API = "https://api.tidal.com/v1";
const SCOPE = "r_usr w_usr w_sub";
const API_TIMEOUT_MS = 15_000;
const SEGMENT_TIMEOUT_MS = 60_000;
const STREAM_IDLE_MS = 60_000;
const STREAM_TOTAL_MS = 25 * 60_000;
const JOB_DEADLINE_MS = 28 * 60_000;
const SEARCH_BUDGET_MS = 45_000;
const ALBUM_CONCURRENCY = 3;
const SEGMENT_WINDOW = 4;
const FLAC_PADDING = 8192;
const CACHE_TTL_MS = 10 * 60_000;
const SEARCH_TTL_MS = 5 * 60_000;
const CACHE_MAX = 200;

export const _config = { baseDelayMs: 1000 };

// The process lives until 5 idle minutes, so this saves repeat requests within a burst of jobs.
const memo = new Map();

/** Returns fn()'s promise, shared by every caller for `ttl` ms. A rejection is not cached. */
function cached(key, ttl, fn) {
  const hit = memo.get(key);
  if (hit && hit.expiresAt > Date.now()) return hit.value;
  const value = fn();
  memo.delete(key);
  memo.set(key, { value, expiresAt: Date.now() + ttl });
  while (memo.size > CACHE_MAX) memo.delete(memo.keys().next().value);
  value.catch(() => memo.get(key)?.value === value && memo.delete(key));
  return value;
}

export const _resetCaches = () => {
  memo.clear();
  profileFailure = { at: 0, reason: "" };
};

// ---------------------------------------------------------------- utilities

const sleep = (ms, signal) =>
  new Promise((res, rej) => {
    if (signal?.aborted) return rej(signal.reason);
    const t = setTimeout(res, ms);
    signal?.addEventListener("abort", () => (clearTimeout(t), rej(signal.reason)), { once: true });
  });

const withTimeout = (signal, ms) => (signal ? AbortSignal.any([signal, AbortSignal.timeout(ms)]) : AbortSignal.timeout(ms));

function hostOf(url) {
  try {
    return new URL(url).host;
  } catch {
    return "server";
  }
}

function retryAfterMs(res) {
  const h = res.headers.get("retry-after");
  if (!h) return null;
  const s = Number(h);
  const ms = Number.isFinite(s) ? s * 1000 : Date.parse(h) - Date.now();
  return Number.isFinite(ms) ? Math.min(Math.max(ms, 0), 30_000) : null;
}

/** fetch with timeout and retries (GET only by default) on 429, 5xx and network errors. */
export async function tfetch(url, { method = "GET", headers, body, timeout = API_TIMEOUT_MS, retries, signal, deadline } = {}) {
  const max = retries ?? (method === "GET" ? 3 : 0);
  for (let attempt = 0; ; attempt++) {
    let res = null;
    let err = null;
    try {
      res = await fetch(url, { method, headers, body, signal: withTimeout(signal, timeout) });
    } catch (e) {
      if (signal?.aborted) throw signal.reason ?? e;
      err = e;
    }
    const retryable = err || res.status === 429 || res.status >= 500;
    let wait = _config.baseDelayMs * 2 ** attempt;
    if (res && res.status === 429) wait = retryAfterMs(res) ?? wait;
    const last = !retryable || attempt >= max || (deadline && Date.now() + wait > deadline);
    if (last) {
      if (err) throw new Error(`Couldn't reach ${hostOf(url)}: ${err.cause?.code || err.message}`);
      return res;
    }
    await res?.body?.cancel().catch(() => {});
    await sleep(wait, signal);
  }
}

async function errorFrom(res, what) {
  let detail = "";
  try {
    const j = await res.json();
    detail = j.userMessage || j.error_description || j.error || "";
  } catch {}
  return new Error(`${what} answered ${res.status}${detail ? `: ${detail}` : ""}`);
}

// ---------------------------------------------------------------- file names

export function safeName(s) {
  let out = String(s ?? "")
    .replace(/[\/\\:*?"<>|\u0000-\u001f\u007f]/g, "_")
    .replace(/^[.\s]+/, "")
    .replace(/[.\s]+$/, "");
  if (Buffer.byteLength(out) > 180) {
    const chars = [...out];
    while (Buffer.byteLength(chars.join("")) > 180) chars.pop();
    out = chars.join("").replace(/[.\s]+$/, "");
  }
  return out || "_";
}

export function safeJoin(directory, ...parts) {
  const base = resolve(directory);
  const full = resolve(base, ...parts);
  if (!full.startsWith(base + sep)) throw new Error("Refusing to write outside the download folder");
  return full;
}

// ---------------------------------------------------------------- auth

// Same built-in client as tiddl (Tidal's own app); used when Client ID is left empty.
const DEFAULT_CLIENT = Buffer.from("NE4zbjZRMXg5NUxMNUs3cDtvS09YZkpXMzcxY1g2eGFaMFB5aGdHTkJkTkxsQlpkNEFLS1lvdWdNamlrPQ==", "base64")
  .toString()
  .split(";");
const clientId = (ctx) => String(ctx.settings.clientId ?? "").trim() || DEFAULT_CLIENT[0];
const clientSecret = (ctx) => (String(ctx.settings.clientId ?? "").trim() ? String(ctx.settings.clientSecret ?? "") : DEFAULT_CLIENT[1]);

const loginRequired = () =>
  Object.assign(new Error("Not logged in to Tidal. Press Test on the plugin page to log in to Tidal."), { loginRequired: true });

const authFile = (ctx) => join(ctx.dataFolder, "auth.json");

// Last record read or written per auth file; this process is the file's only writer.
const authMemo = new Map();

export async function loadAuth(ctx) {
  const id = clientId(ctx);
  const file = resolve(authFile(ctx));
  try {
    let a = authMemo.get(file);
    if (!a) {
      a = JSON.parse(await readFile(file, "utf8"));
      if (a) authMemo.set(file, a);
    }
    if (a && a.clientId === id) return { ...a };
  } catch {}
  return { clientId: id };
}

export async function saveAuth(ctx, a) {
  await mkdir(ctx.dataFolder, { recursive: true });
  const tmp = `${authFile(ctx)}.${process.pid}.${Math.random().toString(36).slice(2)}.tmp`;
  await writeFile(tmp, JSON.stringify(a), { mode: 0o600 });
  await rename(tmp, authFile(ctx));
  authMemo.set(resolve(authFile(ctx)), JSON.parse(JSON.stringify(a)));
}

async function updateAuth(ctx, patch) {
  const a = { ...(await loadAuth(ctx)), ...patch };
  await saveAuth(ctx, a);
  return a;
}

function basic(ctx) {
  if (!clientSecret(ctx)) return undefined; // public clients (browser login) have no secret
  return "Basic " + Buffer.from(`${clientId(ctx)}:${clientSecret(ctx)}`).toString("base64");
}

function postForm(url, fields, auth) {
  const headers = { "content-type": "application/x-www-form-urlencoded" };
  if (auth) headers.authorization = auth;
  return tfetch(url, { method: "POST", headers, body: new URLSearchParams(fields).toString() });
}

const refreshing = new Map();

/** Returns the auth record with a valid access token. Pass staleToken after a 401 to force one refresh. */
export async function getToken(ctx, { staleToken } = {}) {
  const a = await loadAuth(ctx);
  if (!a.accessToken || !a.refreshToken) throw loginRequired();
  const stale = staleToken !== undefined && a.accessToken === staleToken;
  if (!stale && (a.expiresAt ?? 0) - Date.now() > 60_000) return a;
  const key = resolve(ctx.dataFolder);
  if (!refreshing.has(key)) {
    refreshing.set(
      key,
      refresh(ctx, a).finally(() => refreshing.delete(key)),
    );
  }
  return refreshing.get(key);
}

async function refresh(ctx, a) {
  const res = await postForm(
    `${AUTH}/token`,
    { client_id: clientId(ctx), refresh_token: a.refreshToken, grant_type: "refresh_token", scope: SCOPE },
    basic(ctx),
  );
  if (res.status === 400 || res.status === 401) {
    const { accessToken, refreshToken, expiresAt, ...rest } = a;
    await saveAuth(ctx, rest);
    throw loginRequired();
  }
  if (!res.ok) throw await errorFrom(res, "Tidal login");
  const j = await res.json();
  const next = {
    ...(await loadAuth(ctx)),
    accessToken: j.access_token,
    refreshToken: j.refresh_token || a.refreshToken,
    expiresAt: Date.now() + (j.expires_in ?? 3600) * 1000,
  };
  await saveAuth(ctx, next);
  return next;
}

function countryOf(ctx, a) {
  const s = String(ctx.settings.countryCode ?? "").trim().toUpperCase();
  return s || a.countryCode || "US";
}

/** GET a Tidal API path as JSON, refreshing the token once on 401. */
export async function api(ctx, path, params = {}, opts = {}) {
  let a = await getToken(ctx);
  const qs = new URLSearchParams({ countryCode: countryOf(ctx, a), ...params });
  const url = `${API}${path}?${qs}`;
  const call = (tok) => tfetch(url, { ...opts, headers: { authorization: `Bearer ${tok}` } });
  let res = await call(a.accessToken);
  if (res.status === 401 && (await isTokenError(res))) {
    await res.body?.cancel().catch(() => {});
    a = await getToken(ctx, { staleToken: a.accessToken });
    res = await call(a.accessToken);
  }
  if (!res.ok) throw Object.assign(await errorFrom(res, `Tidal ${path.split("/").slice(0, 2).join("/")}`), { status: res.status });
  return res.json();
}

/** Tidal token errors are subStatus 11xxx; other 401s (4005 asset not ready) won't be fixed by a refresh. */
async function isTokenError(res) {
  try {
    const { subStatus } = await res.clone().json();
    return !subStatus || Math.floor(subStatus / 1000) === 11;
  } catch {
    return true; // unreadable body: keep the refresh-and-retry behaviour
  }
}

// Browser (PKCE) login for client IDs Tidal doesn't allow to use the device flow.
const PKCE_REDIRECT = "https://tidal.com/android/login/auth";
const b64url = (b) => b.toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

function pkceUrl(ctx, pkce) {
  const q = new URLSearchParams({
    response_type: "code",
    redirect_uri: PKCE_REDIRECT,
    client_id: clientId(ctx),
    lang: "EN",
    appMode: "android",
    client_unique_key: pkce.uniqueKey,
    code_challenge: b64url(createHash("sha256").update(pkce.verifier).digest()),
    code_challenge_method: "S256",
    restrict_signup: "true",
  });
  return `https://login.tidal.com/authorize?${q}`;
}

const pkceMessage = (url, prefix = "") =>
  `${prefix}Open ${url} and sign in. You land on a page that may show an error, which is expected: copy that page's whole address into Login URL, save, then press Test again.`;

async function startPkce(ctx, a, prefix) {
  const pkce = { verifier: b64url(randomBytes(32)), uniqueKey: randomBytes(8).toString("hex"), createdAt: Date.now() };
  const { device, ...rest } = a;
  await saveAuth(ctx, { ...rest, pkce });
  return { message: pkceMessage(pkceUrl(ctx, pkce), prefix) };
}

function loginCode(settings) {
  const v = String(settings.loginUrl ?? "").trim();
  if (!v) return null;
  const m = v.match(/[?&#]code=([^&#\s]+)/);
  if (m) return decodeURIComponent(m[1]);
  return /^[\w.~-]+$/.test(v) ? v : null;
}

async function exchangePkce(ctx, a, code) {
  const res = await postForm(`${AUTH}/token`, {
    code,
    client_id: clientId(ctx),
    grant_type: "authorization_code",
    redirect_uri: PKCE_REDIRECT,
    scope: SCOPE,
    code_verifier: a.pkce.verifier,
    client_unique_key: a.pkce.uniqueKey,
  });
  const j = await res.json().catch(() => ({}));
  if (res.status === 400 || res.status === 401) return startPkce(ctx, a, "That login code didn't work (it may be used or expired). ");
  if (!res.ok) throw new Error(`Tidal login answered ${res.status}${j.error ? ` (${j.error})` : ""}`);
  await saveTokens(ctx, a, j);
  return connected(ctx);
}

async function saveTokens(ctx, a, j) {
  await saveAuth(ctx, {
    clientId: a.clientId,
    accessToken: j.access_token,
    refreshToken: j.refresh_token,
    expiresAt: Date.now() + (j.expires_in ?? 3600) * 1000,
    userId: j.user_id ?? j.user?.userId,
    username: j.user?.username || j.user?.email || undefined,
    countryCode: j.user?.countryCode,
  });
}

async function startDevice(ctx, a) {
  const res = await postForm(`${AUTH}/device_authorization`, { client_id: clientId(ctx), scope: SCOPE });
  if (res.status === 400 || res.status === 401) {
    const err = await errorFrom(res, "Tidal login");
    if (/limited input device/i.test(err.message)) return startPkce(ctx, a);
    throw err;
  }
  if (!res.ok) throw await errorFrom(res, "Tidal login");
  const j = await res.json();
  const link = j.verificationUriComplete || `${j.verificationUri}/${j.userCode}`;
  const url = /^https?:\/\//.test(link) ? link : `https://${link}`;
  const device = { deviceCode: j.deviceCode, userCode: j.userCode, url, expiresAt: Date.now() + (j.expiresIn ?? 300) * 1000, interval: j.interval ?? 2 };
  await saveAuth(ctx, { ...a, device });
  const mins = Math.round((j.expiresIn ?? 300) / 60);
  return { message: `Open ${url}, approve the login, then press Test again. The code expires in ${mins} minutes.` };
}

async function connected(ctx) {
  const s = await api(ctx, "/sessions");
  const a = await loadAuth(ctx);
  if (s.countryCode && s.countryCode !== a.countryCode) await updateAuth(ctx, { countryCode: s.countryCode });
  const quality = await qualityLine(ctx);
  return { message: `Connected to Tidal as ${a.username || s.userId || a.userId}, country ${countryOf(ctx, { countryCode: s.countryCode || a.countryCode })}. ${quality}` };
}

async function test(ctx) {
  let a = await loadAuth(ctx);
  if (a.accessToken) {
    try {
      return await connected(ctx);
    } catch (e) {
      if (!e.loginRequired) throw e;
      a = await loadAuth(ctx);
    }
  }
  if (a.pkce) {
    const code = loginCode(ctx.settings);
    return code ? exchangePkce(ctx, a, code) : { message: pkceMessage(pkceUrl(ctx, a.pkce)) };
  }
  if (!a.device || a.device.expiresAt <= Date.now()) return startDevice(ctx, a);
  const res = await postForm(
    `${AUTH}/token`,
    { client_id: clientId(ctx), device_code: a.device.deviceCode, grant_type: "urn:ietf:params:oauth:grant-type:device_code", scope: SCOPE },
    basic(ctx),
  );
  const j = await res.json().catch(() => ({}));
  if (res.ok) {
    await saveTokens(ctx, a, j);
    return connected(ctx);
  }
  switch (j.error) {
    case "authorization_pending":
    case "slow_down":
      return { message: `Still waiting for approval at ${a.device.url}. Approve the login there, then press Test again.` };
    case "expired_token":
      return startDevice(ctx, { ...a, device: undefined });
    case "access_denied": {
      const { device, ...rest } = a;
      await saveAuth(ctx, rest);
      throw new Error("The Tidal login was denied. Press Test to start again.");
    }
    default:
      throw new Error(`Tidal login answered ${res.status}${j.error ? ` (${j.error})` : ""}`);
  }
}

// ---------------------------------------------------------------- quality

const RANK = { LOW: 0, HIGH: 1, LOSSLESS: 2, HI_RES_LOSSLESS: 3 };
const TIERS = ["flac-hires", "flac-standard", "mp3-320", "m4a-320", "mp3-256", "m4a-256", "mp3-192", "m4a-192", "mp3-128", "m4a-128"];
const TIER_INFO = {
  HI_RES_LOSSLESS: { tier: "flac-hires", format: "flac", bitrate: 2304 },
  LOSSLESS: { tier: "flac-standard", format: "flac", bitrate: 1411 },
  HIGH: { tier: "m4a-320", format: "m4a", bitrate: 320 },
};

export function parseQuality(s) {
  const v = String(s ?? "").trim().toUpperCase().replace(/[-\s]/g, "_");
  if (!v) return "LOSSLESS";
  if (["HI_RES_LOSSLESS", "HIRES_LOSSLESS", "HIRES", "HI_RES", "MAX"].includes(v)) return "HI_RES_LOSSLESS";
  if (["LOSSLESS", "FLAC", "CD"].includes(v)) return "LOSSLESS";
  if (["HIGH", "AAC", "320"].includes(v)) return "HIGH";
  throw new Error("Quality must be HI_RES_LOSSLESS, LOSSLESS or HIGH");
}

export function trackMaxQuality(t) {
  const tags = t?.mediaMetadata?.tags ?? [];
  if (tags.includes("HIRES_LOSSLESS") || t?.audioQuality === "HI_RES_LOSSLESS") return "HI_RES_LOSSLESS";
  if (t?.audioQuality === "LOSSLESS" || t?.audioQuality === "HI_RES" || tags.includes("LOSSLESS")) return "LOSSLESS";
  if (t?.audioQuality === "HIGH") return "HIGH";
  return "LOW";
}

export const effectiveQuality = (setting, max) => (RANK[setting] <= RANK[max] ? setting : max);

export function isBetter(a, b) {
  if (!a) return false;
  if (b == null) return true;
  const ia = TIERS.indexOf(a);
  const ib = TIERS.indexOf(b);
  return ia !== -1 && (ib === -1 || ia < ib);
}

const TIDAL_QUALITIES = ["HI_RES_LOSSLESS", "LOSSLESS", "HIGH"];
const QUALITY_OF_TIER = Object.fromEntries(Object.entries(TIER_INFO).map(([q, i]) => [i.tier, q]));

/** The typed Quality followed by every quality below it, best first. */
export const typedWanted = (s) => {
  const q = parseQuality(s);
  return TIDAL_QUALITIES.filter((x) => RANK[x] <= RANK[q]);
};

/** The Tidal qualities Aurral's profile enables, in the profile's order. */
export const profileWanted = (profile) =>
  profile.order.filter((t) => profile.enabled.includes(t) && QUALITY_OF_TIER[t]).map((t) => QUALITY_OF_TIER[t]);

/** The first wanted quality the item has (hi-res skipped when the account lacks it), or null. */
export const pickQuality = (wanted, max, hiresCapable) =>
  wanted.find((q) => RANK[q] <= RANK[max] && !(q === "HI_RES_LOSSLESS" && hiresCapable === false)) ?? null;

/** Port of Aurral's isQualityUpgrade (qualityProfileModel.ts); update together. */
export function isUpgrade(tier, currentTier, profile) {
  const { order, enabled, cutoff } = profile;
  if (!enabled.includes(tier)) return false;
  if (!currentTier) return order.indexOf(tier) !== -1 && order.indexOf(tier) <= order.indexOf(cutoff);
  if (!enabled.includes(currentTier)) return true;
  return order.indexOf(tier) < order.indexOf(currentTier);
}

/** Quality, tier, format and bitrate to advertise for a track or album, or null when no wanted quality fits. */
export function advertisedTier(item, wanted, hiresCapable) {
  const q = pickQuality(wanted, trackMaxQuality(item), hiresCapable);
  return q ? { quality: q, ...TIER_INFO[q] } : null;
}

// ---------------------------------------------------------------- ISO BMFF boxes

export function box(type, ...parts) {
  const body = Buffer.concat(parts);
  const head = Buffer.alloc(8);
  head.writeUInt32BE(8 + body.length);
  head.write(type, 4, "latin1");
  return Buffer.concat([head, body]);
}

export function readBoxes(buf, start = 0, end = buf.length) {
  const out = [];
  let p = start;
  while (p + 8 <= end) {
    let size = buf.readUInt32BE(p);
    const type = buf.toString("latin1", p + 4, p + 8);
    let header = 8;
    if (size === 1) {
      if (p + 16 > end) break;
      size = Number(buf.readBigUInt64BE(p + 8));
      header = 16;
    } else if (size === 0) size = end - p;
    if (size < header || p + size > end) break;
    out.push({ type, start: p, header, end: p + size, size });
    p += size;
  }
  return out;
}

// Bytes to skip inside a box before its children start.
const CONTAINERS = { moov: 0, trak: 0, mdia: 0, minf: 0, stbl: 0, udta: 0, moof: 0, traf: 0, mvex: 0, edts: 0, stsd: 8, fLaC: 28, mp4a: 28, meta: 4, ilst: 0 };

function children(buf, b) {
  return readBoxes(buf, b.start + b.header + (CONTAINERS[b.type] ?? 0), b.end);
}

function findAll(buf, type, start = 0, end = buf.length, out = []) {
  for (const b of readBoxes(buf, start, end)) {
    if (b.type === type) out.push(b);
    if (b.type in CONTAINERS) findAll(buf, type, b.start + b.header + CONTAINERS[b.type], b.end, out);
  }
  return out;
}

// ---------------------------------------------------------------- MPD

const unxml = (s) => s.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&amp;/g, "&");
const attr = (tag, name) => {
  const m = tag.match(new RegExp(`\\s${name}="([^"]*)"`));
  return m ? unxml(m[1]) : null;
};

export function parseMpd(xml) {
  if (/<ContentProtection/i.test(xml)) throw new Error("This track is encrypted, which the plugin can't download");
  const rep = xml.match(/<Representation\b[^>]*>/)?.[0] ?? "";
  const tpl = xml.match(/<SegmentTemplate\b[^>]*>/)?.[0];
  if (!tpl) throw new Error("Tidal sent a stream manifest the plugin can't read");
  const codecs = attr(rep, "codecs") ?? attr(xml.match(/<AdaptationSet\b[^>]*>/)?.[0] ?? "", "codecs") ?? "";
  const initUrl = attr(tpl, "initialization");
  const media = attr(tpl, "media");
  const startNumber = Number(attr(tpl, "startNumber") ?? 1);
  let count = 0;
  for (const s of xml.match(/<S\b[^>]*\/?>/g) ?? []) count += 1 + Math.max(0, Number(attr(s, "r") ?? 0));
  if (!initUrl || !media || !count) throw new Error("Tidal sent a stream manifest the plugin can't read");
  const segmentUrls = Array.from({ length: count }, (_, i) => media.replace(/\$Number\$/g, String(startNumber + i)));
  return { codecs, initUrl, segmentUrls, bandwidth: Number(attr(rep, "bandwidth") ?? 0), sampleRate: Number(attr(rep, "audioSamplingRate") ?? 0) };
}

// ---------------------------------------------------------------- FLAC

export function flacBlock(type, data, last) {
  const h = Buffer.alloc(4);
  h[0] = (last ? 0x80 : 0) | type;
  h.writeUIntBE(data.length, 1, 3);
  return Buffer.concat([h, data]);
}

function parseBlocks(buf, p) {
  const blocks = [];
  for (;;) {
    if (p + 4 > buf.length) throw new Error("Truncated FLAC metadata");
    const last = (buf[p] & 0x80) !== 0;
    const type = buf[p] & 0x7f;
    const len = buf.readUIntBE(p + 1, 3);
    blocks.push({ type, data: buf.subarray(p + 4, p + 4 + len) });
    p += 4 + len;
    if (last) return { blocks, end: p };
  }
}

export function parseFlac(buf) {
  if (buf.toString("latin1", 0, 4) !== "fLaC") throw new Error("Not a FLAC file");
  const { blocks, end } = parseBlocks(buf, 4);
  return { blocks, audioStart: end, audio: buf.subarray(end) };
}

function vorbisBlock(tags) {
  const vendor = Buffer.from("aurral-tidal");
  const parts = [u32le(vendor.length), vendor, u32le(tags.length)];
  for (const [k, v] of tags) {
    const s = Buffer.from(`${k}=${v}`, "utf8");
    parts.push(u32le(s.length), s);
  }
  return Buffer.concat(parts);
}

function pictureBlock(img, size) {
  const mime = Buffer.from(img[0] === 0x89 ? "image/png" : "image/jpeg");
  return Buffer.concat([u32be(3), u32be(mime.length), mime, u32be(0), u32be(size), u32be(size), u32be(24), u32be(0), u32be(img.length), img]);
}

function buildFlacHeader(kept, tags, cover, padding, coverSize = 0) {
  const blocks = [...kept.filter((b) => b.type === 0), ...kept.filter((b) => ![0, 1, 4, 6].includes(b.type))];
  if (tags.length) blocks.push({ type: 4, data: vorbisBlock(tags) });
  if (cover) blocks.push({ type: 6, data: pictureBlock(cover, coverSize) });
  if (padding) blocks.push({ type: 1, data: Buffer.alloc(padding) });
  return Buffer.concat([Buffer.from("fLaC"), ...blocks.map((b, i) => flacBlock(b.type, b.data, i === blocks.length - 1))]);
}

export function writeFlacTags(buf, tags, cover, coverSize = 0) {
  const { blocks, audio } = parseFlac(buf);
  return Buffer.concat([buildFlacHeader(blocks, tags, cover, 0, coverSize), audio]);
}

export function readFlacTags(buf) {
  const { blocks } = parseFlac(buf);
  const tags = [];
  for (const b of blocks.filter((x) => x.type === 4)) {
    const d = b.data;
    let p = 4 + d.readUInt32LE(0);
    const n = d.readUInt32LE(p);
    p += 4;
    for (let i = 0; i < n; i++) {
      const len = d.readUInt32LE(p);
      const s = d.toString("utf8", p + 4, p + 4 + len);
      p += 4 + len;
      const eq = s.indexOf("=");
      tags.push([s.slice(0, eq), s.slice(eq + 1)]);
    }
  }
  return { tags, pictures: blocks.filter((x) => x.type === 6).length };
}

/** Rewrites a FLAC file's tags without loading its audio into memory. */
async function retagFlacFile(src, dst, tags, cover, coverSize) {
  const fh = await open(src, "r");
  let audioStart;
  const kept = [];
  try {
    const read = async (pos, len) => {
      const b = Buffer.alloc(len);
      const { bytesRead } = await fh.read(b, 0, len, pos);
      if (bytesRead !== len) throw new Error("Tidal sent a truncated FLAC file");
      return b;
    };
    if ((await read(0, 4)).toString("latin1") !== "fLaC") throw new Error("Tidal sent a file that isn't FLAC");
    let p = 4;
    for (;;) {
      const h = await read(p, 4);
      const type = h[0] & 0x7f;
      const len = h.readUIntBE(1, 3);
      if (![1, 4, 6].includes(type)) kept.push({ type, data: await read(p + 4, len) });
      p += 4 + len;
      if (h[0] & 0x80) break;
    }
    audioStart = p;
  } finally {
    await fh.close();
  }
  await writeFile(dst, buildFlacHeader(kept, tags, cover, FLAC_PADDING, coverSize));
  await pipeline(createReadStream(src, { start: audioStart }), createWriteStream(dst, { flags: "a" }));
}

function streamInfoRate(si) {
  return (si[10] << 12) | (si[11] << 4) | (si[12] >> 4);
}

/** Streams fragmented-MP4 FLAC (Tidal DASH) into a native FLAC stream. */
export class FlacRemuxer {
  constructor(init) {
    const dfla = findAll(init, "dfLa")[0];
    if (!dfla) throw new Error("Tidal sent a FLAC stream without its header");
    this.blocks = parseBlocks(init, dfla.start + dfla.header + 4).blocks;
    const si = this.blocks.find((b) => b.type === 0);
    if (!si) throw new Error("Tidal sent a FLAC stream without STREAMINFO");
    this.streamInfo = Buffer.from(si.data);
    this.rate = streamInfoRate(this.streamInfo);
    const mdhd = findAll(init, "mdhd")[0];
    if (mdhd) {
      const o = mdhd.start + mdhd.header;
      this.timescale = init.readUInt32BE(o + (init[o] === 1 ? 20 : 12));
    }
    if (!this.timescale) this.timescale = this.rate;
    const trex = findAll(init, "trex")[0];
    this.trexDuration = trex && trex.end - trex.start >= trex.header + 16 ? init.readUInt32BE(trex.start + trex.header + 12) : 0;
    this.duration = 0;
  }

  header(tags, cover, padding = FLAC_PADDING, coverSize = 0) {
    return buildFlacHeader(this.blocks, tags, cover, padding, coverSize);
  }

  /** Returns the FLAC frames of one or more moof/mdat pairs and counts their samples. */
  segment(buf) {
    const out = [];
    for (const b of readBoxes(buf)) {
      if (b.type === "mdat") out.push(buf.subarray(b.start + b.header, b.end));
      if (b.type !== "moof") continue;
      for (const traf of children(buf, b).filter((x) => x.type === "traf")) {
        let def = this.trexDuration;
        for (const c of children(buf, traf)) {
          const o = c.start + c.header;
          const flags = buf.readUInt32BE(o) & 0xffffff;
          if (c.type === "tfhd") {
            let q = o + 8;
            if (flags & 0x01) q += 8;
            if (flags & 0x02) q += 4;
            if (flags & 0x08) def = buf.readUInt32BE(q);
          } else if (c.type === "trun") {
            const n = buf.readUInt32BE(o + 4);
            let q = o + 8;
            if (flags & 0x01) q += 4;
            if (flags & 0x04) q += 4;
            const per = [0x100, 0x200, 0x400, 0x800].filter((f) => flags & f).length * 4;
            if (flags & 0x100) for (let i = 0; i < n; i++) this.duration += buf.readUInt32BE(q + i * per);
            else this.duration += n * def;
          }
        }
      }
    }
    return Buffer.concat(out);
  }

  get totalSamples() {
    return Math.round((this.duration * this.rate) / this.timescale);
  }

  /** The 5 STREAMINFO bytes holding total_samples and their file offset. */
  patchBytes() {
    const total = this.totalSamples;
    const b = Buffer.from(this.streamInfo.subarray(13, 18));
    b[0] = (b[0] & 0xf0) | (Math.floor(total / 2 ** 32) & 0x0f);
    b.writeUInt32BE(total >>> 0, 1);
    return { position: 8 + 13, bytes: b };
  }

  patch(target) {
    if (!this.duration) return;
    const { position, bytes } = this.patchBytes();
    bytes.copy(target, position);
  }
}

export function fmp4ToFlac(buf) {
  const first = readBoxes(buf).find((b) => ["moof", "styp", "sidx"].includes(b.type));
  const split = first ? first.start : buf.length;
  const rx = new FlacRemuxer(buf.subarray(0, split));
  const out = Buffer.concat([rx.header([], null, 0), rx.segment(buf.subarray(split))]);
  rx.patch(out);
  return out;
}

// ---------------------------------------------------------------- tags

const fmtGain = (v) => `${Number(v).toFixed(2)} dB`;

export function toVorbis(m) {
  const t = [];
  const add = (k, v) => v != null && v !== "" && t.push([k, String(v)]);
  add("TITLE", m.title);
  add("ARTIST", m.artist);
  for (const a of m.artists ?? []) add("ARTISTS", a);
  add("ALBUM", m.album);
  add("ALBUMARTIST", m.albumArtist);
  add("TRACKNUMBER", m.trackNumber);
  add("TRACKTOTAL", m.trackTotal);
  add("DISCNUMBER", m.discNumber);
  add("DISCTOTAL", m.discTotal);
  add("DATE", m.date);
  add("ISRC", m.isrc);
  add("COPYRIGHT", m.copyright);
  add("BARCODE", m.upc);
  if (m.replayGain != null) add("REPLAYGAIN_TRACK_GAIN", fmtGain(m.replayGain));
  if (m.peak != null) add("REPLAYGAIN_TRACK_PEAK", Number(m.peak).toFixed(6));
  add("LYRICS", m.lyrics);
  add("MUSICBRAINZ_TRACKID", m.mbids?.trackMbid);
  add("MUSICBRAINZ_ARTISTID", m.mbids?.artistMbid);
  add("MUSICBRAINZ_ALBUMID", m.mbids?.releaseMbid);
  return t;
}

const u32be = (n) => {
  const b = Buffer.alloc(4);
  b.writeUInt32BE(n >>> 0);
  return b;
};
const u32le = (n) => {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(n >>> 0);
  return b;
};
const u16be = (n) => {
  const b = Buffer.alloc(2);
  b.writeUInt16BE(Math.min(Number(n) || 0, 0xffff));
  return b;
};

function ilstItems(m, cover) {
  const items = [];
  const text = (name, v) => v != null && v !== "" && items.push(box(name, box("data", u32be(1), u32be(0), Buffer.from(String(v), "utf8"))));
  const free = (name, v) =>
    v != null &&
    v !== "" &&
    items.push(box("----", box("mean", u32be(0), Buffer.from("com.apple.iTunes")), box("name", u32be(0), Buffer.from(name)), box("data", u32be(1), u32be(0), Buffer.from(String(v), "utf8"))));
  text("©nam", m.title);
  text("©ART", m.artist);
  text("aART", m.albumArtist);
  text("©alb", m.album);
  if (m.trackNumber) items.push(box("trkn", box("data", u32be(0), u32be(0), Buffer.alloc(2), u16be(m.trackNumber), u16be(m.trackTotal), Buffer.alloc(2))));
  if (m.discNumber) items.push(box("disk", box("data", u32be(0), u32be(0), Buffer.alloc(2), u16be(m.discNumber), u16be(m.discTotal))));
  text("©day", m.date);
  text("©lyr", m.lyrics);
  text("cprt", m.copyright);
  if (cover) items.push(box("covr", box("data", u32be(cover[0] === 0x89 ? 14 : 13), u32be(0), cover)));
  free("ISRC", m.isrc);
  free("BARCODE", m.upc);
  if (m.replayGain != null) free("replaygain_track_gain", fmtGain(m.replayGain));
  if (m.peak != null) free("replaygain_track_peak", Number(m.peak).toFixed(6));
  free("MusicBrainz Track Id", m.mbids?.trackMbid);
  free("MusicBrainz Artist Id", m.mbids?.artistMbid);
  free("MusicBrainz Album Id", m.mbids?.releaseMbid);
  return items;
}

function udtaFor(m, cover) {
  const hdlr = box("hdlr", Buffer.alloc(8), Buffer.from("mdirappl"), Buffer.alloc(9));
  return box("udta", box("meta", Buffer.alloc(4), hdlr, box("ilst", ...ilstItems(m, cover))));
}

export function writeMp4Tags(buf, m, cover) {
  const top = readBoxes(buf);
  const moov = top.find((b) => b.type === "moov");
  if (!moov) throw new Error("Tidal sent an M4A file without a moov box");
  const inner = children(buf, moov);
  const fragmented = top.some((b) => b.type === "moof") || inner.some((b) => b.type === "mvex");
  if (fragmented) {
    for (const tfhd of findAll(buf, "tfhd")) {
      if (buf.readUInt32BE(tfhd.start + tfhd.header) & 0x000001) throw new Error("Unsupported fragmented M4A layout");
    }
  }
  const newMoov = box("moov", ...inner.filter((b) => b.type !== "udta").map((b) => buf.subarray(b.start, b.end)), udtaFor(m, cover));
  const delta = newMoov.length - moov.size;
  if (delta && top.some((b) => b.type === "mdat" && b.start > moov.start)) {
    for (const t of findAll(newMoov, "stco")) {
      const o = t.start + t.header;
      const n = newMoov.readUInt32BE(o + 4);
      for (let i = 0; i < n; i++) {
        const q = o + 8 + i * 4;
        const v = newMoov.readUInt32BE(q);
        if (v >= moov.start) newMoov.writeUInt32BE(v + delta, q);
      }
    }
    for (const t of findAll(newMoov, "co64")) {
      const o = t.start + t.header;
      const n = newMoov.readUInt32BE(o + 4);
      for (let i = 0; i < n; i++) {
        const q = o + 8 + i * 8;
        const v = newMoov.readBigUInt64BE(q);
        if (v >= BigInt(moov.start)) newMoov.writeBigUInt64BE(v + BigInt(delta), q);
      }
    }
  }
  return Buffer.concat([buf.subarray(0, moov.start), newMoov, buf.subarray(moov.end)]);
}

export function readMp4Tags(buf) {
  const tags = [];
  let pictures = 0;
  const ilst = findAll(buf, "ilst")[0];
  if (!ilst) return { tags, pictures };
  for (const item of readBoxes(buf, ilst.start + ilst.header, ilst.end)) {
    const kids = readBoxes(buf, item.start + item.header, item.end);
    const data = kids.find((k) => k.type === "data");
    if (!data) continue;
    const v = buf.subarray(data.start + data.header + 8, data.end);
    if (item.type === "covr") pictures++;
    else if (item.type === "trkn" || item.type === "disk") tags.push([item.type, `${v.readUInt16BE(2)}/${v.readUInt16BE(4)}`]);
    else if (item.type === "----") {
      const name = kids.find((k) => k.type === "name");
      tags.push([buf.toString("utf8", name.start + name.header + 4, name.end), v.toString("utf8")]);
    } else tags.push([item.type, v.toString("utf8")]);
  }
  return { tags, pictures };
}

// ---------------------------------------------------------------- downloads

async function fetchBuffer(url, signal, timeout = SEGMENT_TIMEOUT_MS) {
  for (let attempt = 0; ; attempt++) {
    try {
      const res = await tfetch(url, { signal, timeout });
      if (!res.ok) throw Object.assign(new Error(`${hostOf(url)} answered ${res.status}`), { status: res.status });
      return Buffer.from(await res.arrayBuffer());
    } catch (e) {
      if (signal?.aborted || e.status || attempt >= 2) throw signal?.reason ?? e;
      await sleep(_config.baseDelayMs * 2 ** attempt, signal);
    }
  }
}

async function streamToFile(url, file, signal) {
  for (let attempt = 0; ; attempt++) {
    const idle = new AbortController();
    let timer;
    const bump = () => {
      clearTimeout(timer);
      timer = setTimeout(() => idle.abort(new Error("The Tidal stream stalled for 60 seconds")), STREAM_IDLE_MS);
    };
    const sig = signal ? AbortSignal.any([signal, idle.signal]) : idle.signal;
    bump();
    try {
      const res = await tfetch(url, { signal: sig, timeout: STREAM_TOTAL_MS });
      if (!res.ok) throw Object.assign(new Error(`${hostOf(url)} answered ${res.status}`), { status: res.status });
      const tick = new Transform({
        transform(chunk, _e, cb) {
          bump();
          cb(null, chunk);
        },
      });
      await pipeline(Readable.fromWeb(res.body), tick, createWriteStream(file), { signal: sig });
      return;
    } catch (e) {
      if (signal?.aborted || e.status || attempt >= 2) throw signal?.reason ?? (idle.signal.aborted ? idle.signal.reason : e);
      await sleep(_config.baseDelayMs * 2 ** attempt, signal);
    } finally {
      clearTimeout(timer);
    }
  }
}

function coverSizeOf(settings) {
  const n = Number(settings.coverSize) || 1280;
  return [80, 160, 320, 640, 1280].reduce((best, s) => (Math.abs(s - n) < Math.abs(best - n) ? s : best), 1280);
}

async function bestEffort(what, fn) {
  try {
    return await fn();
  } catch (e) {
    if (e.status !== 404) console.log(`[tidal] ${what} skipped: ${e.message}`);
    return null;
  }
}

// Cached requests are shared across jobs, so they take no job signal and rely on their own timeouts.
const scope = (ctx) => `${resolve(ctx.dataFolder)}|${String(ctx.settings.countryCode ?? "").trim().toUpperCase()}`;

function getAlbum(ctx, id) {
  return cached(`${scope(ctx)}|album|${id}`, CACHE_TTL_MS, () => api(ctx, `/albums/${id}`));
}

function getCover(ctx, album) {
  if (ctx.settings.skipCover || !album.cover) return null;
  const size = coverSizeOf(ctx.settings);
  const url = `https://resources.tidal.com/images/${album.cover.replace(/-/g, "/")}/${size}x${size}.jpg`;
  return bestEffort("cover", () => cached(`cover|${url}`, CACHE_TTL_MS, () => fetchBuffer(url, undefined, 20_000)));
}

function getLyrics(ctx, trackId) {
  if (ctx.settings.skipLyrics) return null;
  return bestEffort("lyrics", () =>
    cached(`${scope(ctx)}|lyrics|${trackId}`, CACHE_TTL_MS, async () => {
      try {
        return (await api(ctx, `/tracks/${trackId}/lyrics`, {}, { timeout: 10_000, retries: 0 })).lyrics || null;
      } catch (e) {
        if (e.status === 404) return null; // remembered: most tracks have no lyrics
        throw e;
      }
    }),
  );
}

/** Fetches urls up to SEGMENT_WINDOW at a time and yields their bodies in order. */
async function* fetchInOrder(urls, signal) {
  const ac = new AbortController();
  const sig = signal ? AbortSignal.any([signal, ac.signal]) : ac.signal;
  const pending = [];
  let next = 0;
  const fill = () => {
    while (next < urls.length && pending.length < SEGMENT_WINDOW) {
      const p = fetchBuffer(urls[next++], sig);
      p.catch(() => {});
      pending.push(p);
    }
  };
  try {
    fill();
    while (pending.length) {
      const buf = await pending.shift();
      fill();
      yield buf;
    }
  } finally {
    ac.abort(); // stop prefetches after a failure
  }
}

const trackTitle = (t) => (t.version ? `${t.title} (${t.version})` : t.title);

function buildMeta(track, album, lyrics, mbids) {
  const date = album.releaseDate || track.streamStartDate?.slice(0, 10) || null;
  return {
    title: trackTitle(track),
    artist: track.artist?.name || track.artists?.[0]?.name,
    artists: (track.artists ?? []).map((a) => a.name).filter(Boolean),
    album: album.title || track.album?.title,
    albumArtist: album.artist?.name,
    trackNumber: track.trackNumber,
    trackTotal: album.numberOfTracks,
    discNumber: track.volumeNumber,
    discTotal: album.numberOfVolumes,
    date,
    isrc: track.isrc,
    copyright: track.copyright || album.copyright,
    upc: album.upc,
    replayGain: track.replayGain,
    peak: track.peak,
    lyrics,
    mbids,
  };
}

let loggedHiresCap = false;

/**
 * Downloads one track into `directory`. `nameOf(meta, ext)` returns path parts relative to it.
 * Returns the path relative to `directory`.
 */
async function downloadTrack(ctx, trackId, directory, nameOf, { signal, mbids, track } = {}) {
  const t0 = Date.now();
  track ??= await api(ctx, `/tracks/${trackId}`, {}, { signal });
  if (!track.allowStreaming || !track.streamReady) throw new Error(`"${track.title}" isn't available to stream`);
  const [album, auth] = await Promise.all([getAlbum(ctx, track.album.id), loadAuth(ctx)]);
  const max = trackMaxQuality(track);
  // No error when nothing wanted fits: an album downloads every track, and Aurral judges each file against its profile.
  const capped = max === "HI_RES_LOSSLESS" && auth.hiresCapable === false ? "LOSSLESS" : max;
  const requested = pickQuality((await qualityPlan(ctx)).wanted, max, auth.hiresCapable) ?? capped;

  const pb = await api(ctx, `/tracks/${track.id}/playbackinfopostpaywall`, { audioquality: requested, playbackmode: "STREAM", assetpresentation: "FULL" }, { signal });
  const delivered = pb.audioQuality;
  if (requested === "HI_RES_LOSSLESS" && delivered !== "HI_RES_LOSSLESS" && max === "HI_RES_LOSSLESS") {
    await updateAuth(ctx, { hiresCapable: false });
    if (!loggedHiresCap) console.log(`[tidal] Tidal delivered ${delivered} for a hi-res track; this account or client ID isn't entitled to hi-res, so results are now capped at LOSSLESS`);
    loggedHiresCap = true;
  }

  const [lyrics, cover] = await Promise.all([getLyrics(ctx, track.id), getCover(ctx, album)]);
  const meta = buildMeta(track, album, lyrics, mbids);
  const tags = toVorbis(meta);
  const coverSize = coverSizeOf(ctx.settings);
  const manifest = Buffer.from(pb.manifest, "base64").toString("utf8");

  let final;
  let part;
  const target = async (ext) => {
    final = safeJoin(directory, ...nameOf(meta, ext));
    part = `${final}.part`;
    await mkdir(dirname(final), { recursive: true });
  };
  const raw = () => `${part}.raw`;
  try {
    if (pb.manifestMimeType === "application/vnd.tidal.bts") {
      const m = JSON.parse(manifest);
      if (m.encryptionType && m.encryptionType !== "NONE") throw new Error("This track is encrypted, which the plugin can't download");
      const url = m.urls?.[0];
      if (!url) throw new Error("Tidal sent no stream URL");
      const isFlac = /flac/i.test(m.codecs || "") || /flac/i.test(m.mimeType || "");
      if (isFlac && /mp4/i.test(m.mimeType || "")) throw new Error("Tidal sent FLAC in an MP4 container, which the plugin can't handle yet");
      if (isFlac) {
        await target("flac");
        await streamToFile(url, raw(), signal);
        await retagFlacFile(raw(), part, tags, cover, coverSize);
      } else {
        await target("m4a");
        await writeFile(part, writeMp4Tags(await fetchBuffer(url, signal, STREAM_TOTAL_MS), meta, cover));
      }
    } else if (pb.manifestMimeType === "application/dash+xml") {
      const mpd = parseMpd(manifest);
      const init = await fetchBuffer(mpd.initUrl, signal);
      if (/flac/i.test(mpd.codecs)) {
        await target("flac");
        const rx = new FlacRemuxer(init);
        const fh = await open(part, "w");
        try {
          await fh.write(rx.header(tags, cover, FLAC_PADDING, coverSize));
          for await (const seg of fetchInOrder(mpd.segmentUrls, signal)) await fh.write(rx.segment(seg));
          if (rx.duration) {
            const { position, bytes } = rx.patchBytes();
            await fh.write(bytes, 0, bytes.length, position);
          }
        } finally {
          await fh.close();
        }
      } else if (/mp4a/i.test(mpd.codecs)) {
        await target("m4a");
        const segs = [init];
        for await (const seg of fetchInOrder(mpd.segmentUrls, signal)) segs.push(seg);
        await writeFile(part, writeMp4Tags(Buffer.concat(segs), meta, cover));
      } else throw new Error(`Tidal sent a stream in ${mpd.codecs || "an unknown codec"}, which the plugin can't handle`);
    } else throw new Error(`Tidal sent a ${pb.manifestMimeType} manifest, which the plugin can't read`);

    await rename(part, final);
    const { size } = await stat(final);
    console.log(
      `[tidal] ${meta.artist} - ${meta.title}: ${requested} → ${delivered}, ${size} bytes, ${tags.length} tags, cover ${cover ? "yes" : "no"}, ${Date.now() - t0} ms`,
    );
    return final.slice(resolve(directory).length + 1);
  } finally {
    if (part) {
      await rm(part, { force: true });
      await rm(raw(), { force: true });
    }
  }
}

async function withDeadline(ms, message, fn) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(Object.assign(new Error(message), { deadline: true })), ms);
  try {
    return await fn(ac);
  } catch (e) {
    throw ac.signal.aborted && ac.signal.reason?.deadline ? ac.signal.reason : e;
  } finally {
    clearTimeout(timer);
  }
}

async function clearDirectory(directory) {
  for (const name of await readdir(directory).catch(() => [])) await rm(join(directory, name), { recursive: true, force: true });
}

// ---------------------------------------------------------------- download source

const PROFILE_TTL_MS = 5 * 60_000;
const PROFILE_BACKOFF_MS = 60_000;
let profileFailure = { at: 0, reason: "" };

const profileKey = (conn) => `aurral|${conn.url}|qualityProfile`;

/** Aurral's quality profile from its admin `/settings`; nothing else in that answer (it holds other services' secrets) is kept. */
function aurralProfile(ctx) {
  const conn = ctx.api;
  if (!conn?.url || !conn?.key) return Promise.reject(new Error("API access is off"));
  return cached(profileKey(conn), PROFILE_TTL_MS, async () => {
    const res = await tfetch(`${conn.url.replace(/\/+$/, "")}/settings`, { headers: { "X-Api-Key": conn.key }, timeout: 5000, retries: 0 });
    if (!res.ok) throw await errorFrom(res, "Aurral");
    const p = (await res.json().catch(() => null))?.qualityProfile;
    if (!p || typeof p !== "object" || !Array.isArray(p.order) || !Array.isArray(p.enabled) || typeof p.cutoff !== "string") {
      throw new Error("Aurral's answer has no valid quality profile");
    }
    return { order: p.order, enabled: p.enabled, cutoff: p.cutoff };
  });
}

/** Which qualities to ask for, best first, and where that choice came from. */
async function qualityPlan(ctx) {
  const { hiresCapable } = await loadAuth(ctx);
  const typed = (error) => ({ wanted: typedWanted(ctx.settings.quality), hiresCapable, profile: null, source: "Quality setting", error });
  if (!ctx.settings.autoQuality) return typed();
  if (Date.now() - profileFailure.at < PROFILE_BACKOFF_MS) return typed(profileFailure.reason);
  try {
    const profile = await aurralProfile(ctx);
    return { wanted: profileWanted(profile), hiresCapable, profile, source: "Aurral profile" };
  } catch (e) {
    // Parallel callers share one failed request: log it once.
    if (Date.now() - profileFailure.at >= PROFILE_BACKOFF_MS) {
      profileFailure = { at: Date.now(), reason: e.message };
      const fallback = typed(e.message);
      console.log(`[tidal] Couldn't read Aurral's quality profile (${e.message}); using Quality ${fallback.wanted[0]}`);
      return fallback;
    }
    return typed(profileFailure.reason);
  }
}

const upgrades = (plan, tier, currentTier) => (plan.profile ? isUpgrade(tier, currentTier, plan.profile) : isBetter(tier, currentTier));

/** The Test message's quality sentence. */
async function qualityLine(ctx) {
  // Test rereads the profile, so a fix or a profile change shows at once.
  profileFailure = { at: 0, reason: "" };
  if (ctx.api?.url) memo.delete(profileKey(ctx.api));
  let plan;
  try {
    plan = await qualityPlan(ctx);
  } catch (e) {
    return `Quality: ${e.message}`;
  }
  const first = plan.wanted.find((q) => !(q === "HI_RES_LOSSLESS" && plan.hiresCapable === false));
  if (plan.profile) {
    return first ? `Quality: Aurral profile (${first} first)` : "Quality: Aurral profile — no Tidal quality is enabled, so searches return nothing";
  }
  return `Quality: Quality setting (${first})${plan.error ? ` — couldn't read Aurral's profile: ${plan.error}` : ""}`;
}

const playable = (x) => x && x.allowStreaming && x.streamReady;

const norm = (s) =>
  String(s ?? "")
    .normalize("NFKD")
    .replace(/\p{M}/gu, "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, "");

/** True when a Tidal track is the requested recording, so the request's MusicBrainz IDs may be written into it. */
export function matchesRequest(t, track) {
  const artist = norm(track.artist);
  const title = norm(track.title);
  if (!artist || !title || norm(trackTitle(t)) !== title) return false;
  if (![t.artist, ...(t.artists ?? [])].some((a) => a && norm(a.name) === artist)) return false;
  return !(t.duration && track.durationMs) || Math.abs(t.duration * 1000 - track.durationMs) <= 3000;
}

/** The requested artist, whole and split on joiners ("Timbaland Presents OneRepublic" → both). */
const artistParts = (s) =>
  String(s ?? "").split(/\s*(?:,|&|\+|\/|\b(?:feat|ft)\b\.?|\b(?:featuring|presents|with|and|x|vs)\b\.?)\s*/i).map((p) => p.trim()).filter(Boolean);
const artistNames = (s) => new Set([s, ...artistParts(s)].map(norm).filter(Boolean));

/** Aurral repeats the same search across sources, upgrade passes and Manual Search. */
const searchApi = (ctx, params, deadline) =>
  cached(`${scope(ctx)}|search|${new URLSearchParams(params)}`, SEARCH_TTL_MS, () => api(ctx, "/search", params, { deadline }));

/** The fields downloadTrack reads, carried in result.data so download needs no /tracks/{id} call. */
const trackSnapshot = (t) => ({
  id: t.id,
  title: t.title,
  version: t.version,
  duration: t.duration,
  allowStreaming: t.allowStreaming,
  streamReady: t.streamReady,
  audioQuality: t.audioQuality,
  mediaMetadata: t.mediaMetadata,
  artist: t.artist,
  artists: t.artists,
  album: t.album && { id: t.album.id, title: t.album.title, cover: t.album.cover },
  trackNumber: t.trackNumber,
  volumeNumber: t.volumeNumber,
  isrc: t.isrc,
  copyright: t.copyright,
  replayGain: t.replayGain,
  peak: t.peak,
  streamStartDate: t.streamStartDate,
});

async function searchTracks(input) {
  const { track, upgrade } = input;
  const plan = await qualityPlan(input);
  const deadline = Date.now() + SEARCH_BUDGET_MS;
  // Tidal can miss "Deutschland (Live in Mexico City)" yet find it for "Rammstein Deutschland", so retry without
  // the bracketed version, then the bare title, until a result has the requested title.
  const base = String(track.title ?? "").replace(/\s*[([][^)\]]*[)\]]/g, "").trim();
  const queries = [[track.artist, track.title].filter(Boolean).join(" ")];
  if (track.artist && base && base !== track.title) queries.push(`${track.artist} ${base}`);
  if (track.artist && track.title) queries.push(track.title);
  // The bare-title query returns other artists' songs, so an exact title only counts when it's by the requested artist.
  const wanted = artistNames(track.artist);
  const exact = (t) =>
    norm(trackTitle(t)) === norm(track.title) && (!wanted.size || [t.artist, ...(t.artists ?? [])].some((a) => a && wanted.has(norm(a.name))));
  const found = new Map();
  for (const query of queries) {
    if (!query.trim()) continue;
    const r = await searchApi(input, { query, types: "TRACKS", limit: "50" }, deadline);
    for (const t of (r.tracks?.items ?? r.items ?? []).filter(playable)) if (!found.has(t.id)) found.set(t.id, t);
    if ([...found.values()].some(exact)) break;
  }
  // Exact titles first, so the 50-result cap never drops one a later query found.
  const items = [...found.values()].sort((x, y) => exact(y) - exact(x));
  const mbids = { trackMbid: track.trackMbid, artistMbid: track.artistMbid, releaseMbid: track.releaseMbid };
  const out = [];
  for (const t of items) {
    const tier = advertisedTier(t, plan.wanted, plan.hiresCapable);
    if (!tier) continue;
    if (upgrade && !upgrades(plan, tier.tier, upgrade.currentTier)) continue;
    out.push({
      id: String(t.id),
      title: trackTitle(t),
      // Aurral splits the credit on commas and matches any name, so a duet matches either artist.
      artist: [...new Set((t.artists ?? []).map((a) => a?.name).filter(Boolean))].join(", ") || t.artist?.name || "",
      album: t.album?.title ?? null,
      durationMs: t.duration ? t.duration * 1000 : null,
      format: tier.format,
      bitrate: tier.bitrate,
      data: { trackId: t.id, albumId: t.album?.id ?? null, mbids: matchesRequest(t, track) ? mbids : null, track: t.album?.id ? trackSnapshot(t) : undefined },
    });
  }
  return out.slice(0, 50);
}

async function download(input) {
  const { result, directory } = input;
  const trackId = result.data?.trackId ?? result.id;
  const mbids = result.data?.mbids;
  return withDeadline(JOB_DEADLINE_MS, "Download took longer than 28 minutes", async (ac) => {
    const path = await downloadTrack(input, trackId, directory, (m, ext) => [`${safeName(`${m.artist} - ${m.title}`)}.${ext}`], { signal: ac.signal, mbids, track: result.data?.track });
    return { path };
  });
}

async function searchAlbum(input) {
  const { album, upgrade } = input;
  const plan = await qualityPlan(input);
  const query = [album.artist, album.title].filter(Boolean).join(" ").trim();
  if (!query) return [];
  // Aurral tries the first albums as listed without matching them, so drop other artists' karaoke and tribute albums,
  // and put albums with the requested title first.
  const wanted = artistNames(album.artist);
  const byArtist = (a) => !wanted.size || [a.artist, ...(a.artists ?? [])].some((x) => x && wanted.has(norm(x.name)));
  const titled = (a) => !!album.title && norm(a.title) === norm(album.title);
  // Tidal can miss the album for a joined artist ("Timbaland Presents OneRepublic"), so retry with each name on its own.
  const parts = artistParts(album.artist);
  const queries = [query, ...(album.title && parts.length > 1 ? parts.map((p) => `${p} ${album.title}`) : [])];
  const deadline = Date.now() + SEARCH_BUDGET_MS;
  const found = new Map();
  for (const q of queries) {
    const r = await searchApi(input, { query: q, types: "ALBUMS", limit: "50" }, deadline);
    for (const a of (r.albums?.items ?? r.items ?? []).filter(playable).filter(byArtist)) if (!found.has(a.id)) found.set(a.id, a);
    if ([...found.values()].some(titled)) break;
  }
  const albums = [...found.values()].sort((x, y) => titled(y) - titled(x));
  const out = [];
  for (const a of albums) {
    const tier = advertisedTier(a, plan.wanted, plan.hiresCapable);
    if (!tier) continue;
    if (upgrade && !upgrades(plan, tier.tier, upgrade.currentTier)) continue;
    out.push({
      id: String(a.id),
      title: a.version ? `${a.title} (${a.version})` : a.title,
      artist: a.artist?.name || a.artists?.[0]?.name || "",
      year: a.releaseDate ? Number(a.releaseDate.slice(0, 4)) || null : null,
      trackCount: a.numberOfTracks ?? null,
      format: tier.format,
      bitrate: tier.bitrate,
      data: { albumId: a.id },
    });
  }
  return out.slice(0, 50);
}

async function albumTracks(ctx, albumId, signal) {
  const tracks = [];
  for (let offset = 0; ; offset += 100) {
    const page = await api(ctx, `/albums/${albumId}/items`, { limit: "100", offset: String(offset) }, { signal });
    const items = page.items ?? [];
    for (const it of items) if (it.type === "track" && it.item) tracks.push(it.item);
    if (!items.length || offset + items.length >= (page.totalNumberOfItems ?? 0)) return tracks;
  }
}

async function downloadAlbum(input) {
  const { result, directory } = input;
  const albumId = result.data?.albumId ?? result.id;
  const pad = (n) => String(n ?? 0).padStart(2, "0");
  try {
    await withDeadline(JOB_DEADLINE_MS, "Album took longer than 28 minutes", async (ac) => {
      const tracks = (await albumTracks(input, albumId, ac.signal)).filter(playable);
      if (!tracks.length) throw new Error("This album has no tracks available to stream");
      let next = 0;
      let failure = null;
      const worker = async () => {
        while (!failure && next < tracks.length) {
          const t = tracks[next++];
          try {
            await downloadTrack(input, t.id, directory, (m, ext) => [`Disc ${m.discNumber || 1}`, `${pad(m.trackNumber)} - ${safeName(m.title)}.${ext}`], {
              signal: ac.signal,
              track: t,
            });
          } catch (e) {
            if (!failure) {
              failure = ac.signal.aborted && ac.signal.reason?.deadline ? ac.signal.reason : new Error(`Couldn't download "${pad(t.trackNumber)} - ${trackTitle(t)}": ${e.message}`);
              ac.abort(failure);
            }
          }
        }
      };
      await Promise.all(Array.from({ length: Math.min(ALBUM_CONCURRENCY, tracks.length) }, worker));
      if (failure) throw failure;
    });
  } catch (e) {
    await clearDirectory(directory);
    throw e;
  }
}

export default {
  test,
  downloadSource: { search: searchTracks, download, searchAlbum, downloadAlbum },
};
