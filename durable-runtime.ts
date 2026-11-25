import { createHash } from "node:crypto";
import { access, chmod, mkdir, open, readFile, unlink } from "node:fs/promises";
import { closeSync, openSync, writeSync } from "node:fs";
import { join } from "node:path";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, createProvider, type Api, type AssistantMessageEventStream, type Model, type MutableModels, type ProviderStreams } from "@earendil-works/pi-ai";
import { Harness, MemoryStorage, createRegistry, type Extension, type Harness as DurableHarness, type Registry, type ToolRegistration } from "@earendil-works/pi-durable";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";
import { NodeExecutionEnv } from "@earendil-works/pi-durable/env/node";
import type { ExtensionContext, ModelRegistry } from "@earendil-works/pi-coding-agent";

export type DurableRuntime = { harness: DurableHarness; registry: Registry; models: MutableModels };
type CachedRuntime = DurableRuntime & { sessionId: string; releaseLock: () => Promise<void>; persistent: boolean };
const KEY = Symbol.for("pi-durable-extension.runtimes");
const pending = new Map<string, Promise<CachedRuntime>>();
const closing = new Map<string, Promise<void>>();
function runtimes(): Map<string, CachedRuntime> {
  const global = globalThis as typeof globalThis & { [KEY]?: Map<string, CachedRuntime> };
  return (global[KEY] ??= new Map());
}
function syncModels(target: MutableModels, source: ModelRegistry) {
  const groups = new Map<string, Model<Api>[]>();
  for (const model of source.getAll()) groups.set(model.provider, [...(groups.get(model.provider) ?? []), model]);
  for (const provider of target.getProviders()) if (!groups.has(provider.id)) target.deleteProvider(provider.id);
  for (const [id, models] of groups) {
    const upstream = source.getProvider(id);
    const api: ProviderStreams = {
      stream: (model, context, options) => source.stream(model, context, options) as AssistantMessageEventStream,
      streamSimple: (model, context, options) => source.streamSimple(model, context, options),
    };
    target.setProvider(createProvider<Api>({ id, name: upstream?.name ?? id, baseUrl: upstream?.baseUrl, headers: upstream?.headers,
      auth: { apiKey: {
        name: `Pi session auth (${id})`,
        async check() { return models.some((model) => source.hasConfiguredAuth(model)) ? { type: "api_key", source: "Pi session" } : undefined; },
        async resolve() {
          const model = models.find((candidate) => source.hasConfiguredAuth(candidate));
          if (!model) return undefined;
          const result = await source.getApiKeyAndHeaders(model);
          return result.ok ? { auth: { apiKey: result.apiKey, headers: result.headers, baseUrl: result.baseUrl }, env: result.env, source: "Pi session" } : undefined;
        },
      } }, models, api,
    }));
  }
}
function pathFor(ctx: ExtensionContext) {
  const sessionId = ctx.sessionManager.getSessionId();
  const dir = ctx.sessionManager.getSessionDir();
  const key = createHash("sha256").update(sessionId).digest("hex").slice(0, 24);
  return { sessionId, path: dir ? join(dir, `pi-swarm-${key}.sqlite`) : `memory:pi-swarm:${sessionId}`, persistent: Boolean(dir) };
}
export async function hasRuntime(ctx: ExtensionContext) {
  const { path, persistent } = pathFor(ctx);
  if (runtimes().has(path) || pending.has(path)) return true;
  if (!persistent) return false;
  try { await access(path); return true; }
  catch (error: any) { if (error.code === "ENOENT") return false; throw error; }
}
export async function getRuntime(ctx: ExtensionContext, extensions: readonly Extension<ToolRegistration>[] = []) {
  const { sessionId, path } = pathFor(ctx);
  let found = runtimes().get(path);
  const closingRuntime = closing.get(path);
  if (closingRuntime) await closingRuntime;
  found = runtimes().get(path);
  if (!found) {
    let opening = pending.get(path);
    if (!opening) { opening = createRuntime(ctx, sessionId, path, extensions); pending.set(path, opening); }
    try { found = await opening; } finally { if (pending.get(path) === opening) pending.delete(path); }
  }
  const closingAgain = closing.get(path);
  if (closingAgain) {
    await closingAgain;
    return getRuntime(ctx, extensions);
  }
  if (!found) return getRuntime(ctx, extensions);
  if (found.sessionId !== sessionId) throw new Error("Durable session identity mismatch");
  for (const extension of extensions) found.registry.install(extension);
  syncModels(found.models, ctx.modelRegistry); found.harness.resume(); return found;
}
async function createRuntime(ctx: ExtensionContext, sessionId: string, path: string, extensions: readonly Extension<ToolRegistration>[]): Promise<CachedRuntime> {
  const { persistent } = pathFor(ctx);
  let releaseLock = async () => {};
  try {
    let storage;
    if (persistent) {
      await mkdir(ctx.sessionManager.getSessionDir(), { recursive: true, mode: 0o700 });
      const lockPath = `${path}.lock`;
      let lock: number;
      try { lock = openSync(lockPath, "wx", 0o600); }
      catch (error: any) {
        if (error.code !== "EEXIST") throw error;
        const contents = await readFile(lockPath, "utf8").catch(() => "");
        if (!/^\d+$/.test(contents)) throw new Error("Durable runtime lock is incomplete; refusing to remove it");
        const pid = Number(contents);
        if (!Number.isSafeInteger(pid) || pid < 1) throw new Error("Durable runtime lock has an invalid process ID");
        try { process.kill(pid, 0); throw new Error(`Durable runtime already open for this Pi session (pid ${pid})`); }
        catch (check: any) { if (check.code === "EPERM") throw new Error(`Durable runtime already open for this Pi session (pid ${pid})`); if (check.code !== "ESRCH") throw check; }
        throw new Error(`Stale Durable runtime lock for pid ${pid}; remove ${lockPath} only after confirming its process is gone`);
      }
      try { writeSync(lock, String(process.pid)); }
      catch (error) { closeSync(lock); await unlink(lockPath); throw error; }
      closeSync(lock);
      releaseLock = async () => {
        const pid = Number(await readFile(lockPath, "utf8").catch(() => ""));
        if (pid === process.pid) await unlink(lockPath).catch((error: any) => { if (error.code !== "ENOENT") throw error; });
      };
      const file = await open(path, "a", 0o600);
      await file.close();
      await chmod(path, 0o600);
      storage = await openNodeSqliteStorage(path, { busyTimeoutMs: 5000 });
    } else storage = new MemoryStorage();
    const registry = createRegistry(); for (const extension of extensions) registry.install(extension);
    const models = createModels(); syncModels(models, ctx.modelRegistry);
    const harness = await Harness.open(storage, { models: () => models, registry, env: ({ cwd }) => new NodeExecutionEnv({ cwd: cwd ?? ctx.cwd }), settings: { restartLimit: 3 } }, BACKGROUND_CONTEXT);
    const runtime = { harness, registry, models, sessionId, releaseLock, persistent };
    runtimes().set(path, runtime); harness.resume(); return runtime;
  } catch (error) { await releaseLock(); throw error; }
}
export async function closeRuntime(ctx: ExtensionContext) {
  const { path } = pathFor(ctx);
  const alreadyClosing = closing.get(path);
  if (alreadyClosing) return alreadyClosing;
  let runtime = runtimes().get(path);
  const opening = pending.get(path);
  if (!runtime && opening) runtime = await opening;
  if (!runtime) return;
  runtimes().delete(path);
  const closingRuntime = runtime.harness.close(BACKGROUND_CONTEXT).finally(async () => {
    await runtime!.releaseLock();
    closing.delete(path);
  });
  closing.set(path, closingRuntime);
  await closingRuntime;
}
