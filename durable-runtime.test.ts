import assert from "node:assert/strict";
import test from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { defineDoc } from "@earendil-works/pi-durable";
import { closeRuntime, getRuntime, hasRuntime } from "./durable-runtime.ts";

const TestState = defineDoc<{ value: number }>({ kind: "swarm.runtime-test", version: 1, scope: "session", initial: () => ({ value: 0 }), checkpointWhen: () => false });

function context(sessionId: string) {
  return {
    cwd: process.cwd(),
    sessionManager: { getSessionId: () => sessionId, getSessionDir: () => "" },
    modelRegistry: { getAll: () => [], getProvider: () => undefined },
  } as any;
}

test("no-session runtime uses memory storage and shares concurrent opens", async () => {
  const ctx = context(`memory-${crypto.randomUUID()}`);
  assert.equal(await hasRuntime(ctx), false);
  const [first, second] = await Promise.all([getRuntime(ctx), getRuntime(ctx)]);
  assert.equal(first.harness, second.harness);
  assert.equal(await hasRuntime(ctx), true);
  await first.harness.commit(async (tx) => { (await tx.doc(TestState)).value = 7; }, BACKGROUND_CONTEXT);
  assert.equal((await second.harness.snapshot(TestState, BACKGROUND_CONTEXT))?.value, 7);
  await closeRuntime(ctx);
  assert.equal(await hasRuntime(ctx), false);
});

test("a worker's board wait parks across a restart until a post wakes it", async () => {
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { Harness, createRegistry } = await import("@earendil-works/pi-durable");
  const { openNodeSqliteStorage } = await import("@earendil-works/pi-durable/storage/sqlite/node");
  const { createModels, fauxProvider, fauxAssistantMessage, fauxToolCall } = await import("@earendil-works/pi-ai");
  const { appendMessage, createSwarmExtension, snapshot } = await import("./index.ts");
  const dir = await mkdtemp(join(tmpdir(), "pi-swarm-test-"));
  const faux = fauxProvider();
  const models = createModels();
  models.setProvider(faux.provider);
  faux.setResponses([fauxAssistantMessage(fauxToolCall("swarm", { action: "wait", timeoutMs: 60000 }), { stopReason: "toolUse" }), fauxAssistantMessage("done")]);
  const ref: any = {};
  const swarm = createSwarmExtension(ref);
  const open = async () => {
    const registry = createRegistry();
    registry.install(swarm);
    const harness = await Harness.open(await openNodeSqliteStorage(join(dir, "swarm.sqlite")), { models: () => models, registry }, BACKGROUND_CONTEXT);
    ref.value = { harness, registry, models };
    harness.resume();
    return harness;
  };
  try {
    let harness = await open();
    const model = faux.getModel();
    const worker = await harness.createConversation({ ownership: { kind: "ownerless" }, agent: { model: { provider: model.provider, modelId: model.id }, extensions: [swarm] } }, BACKGROUND_CONTEXT);
    const submissionId = (await worker.submit({ type: "input", content: "wait for the board" }, BACKGROUND_CONTEXT)).id;
    while (!(await harness.inspect(BACKGROUND_CONTEXT)).tasks.some((task) => task.record.state.status === "sleeping")) await new Promise((resolve) => setTimeout(resolve, 10));
    await harness.close(BACKGROUND_CONTEXT);
    harness = await open();
    await appendMessage(ref.value, "coordinator", "main", "go ahead");
    const submission = await harness.submission(submissionId, BACKGROUND_CONTEXT);
    assert.equal((await submission!.wait(BACKGROUND_CONTEXT)).status, "done");
    const resumed = (await harness.conversation(worker.id, BACKGROUND_CONTEXT))!;
    assert.match(JSON.stringify((await resumed.context(BACKGROUND_CONTEXT)).messages), /go ahead/);
    // A wait whose message is already on the board returns at once, without parking.
    faux.appendResponses([fauxAssistantMessage(fauxToolCall("swarm", { action: "wait", since: 0 }), { stopReason: "toolUse" }), fauxAssistantMessage("seen")]);
    assert.equal((await (await resumed.submit({ type: "input", content: "read" }, BACKGROUND_CONTEXT)).wait(BACKGROUND_CONTEXT)).status, "done");
    assert.equal(JSON.stringify((await resumed.context(BACKGROUND_CONTEXT)).messages).match(/#1 @main coordinator: go ahead/g)?.length, 2);
    assert.equal((await snapshot(ref.value)).sequence, 1);
    await harness.close(BACKGROUND_CONTEXT);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("add refuses review and testCommand without pi-worker-guards, and installs its guards in the worker's harness", async () => {
  const { default: swarm } = await import("./index.ts");
  const key = Symbol.for("pi-worker-guards.host");
  const tools: Record<string, any> = {}, on: Record<string, Function> = {};
  swarm({ registerTool: (tool: any) => { tools[tool.name] = tool; }, registerCommand: () => {}, on: (name: string, fn: Function) => { on[name] = fn; }, getSettings: () => ({}), sendMessage: () => {} } as any);
  const ctx = { ...context(`memory-${crypto.randomUUID()}`), model: undefined, thinkingLevel: "medium", modelRegistry: { getAll: () => [], getProvider: () => undefined, find: () => undefined, hasConfiguredAuth: () => false } };
  const add = (params: object) => tools.swarm.execute("id", { action: "add", name: "w", brief: "b", ...params }, undefined, undefined, ctx);
  delete (globalThis as any)[key];
  await assert.rejects(add({ review: true }), /pi-worker-guards/);
  await assert.rejects(add({}), /unknown or unauthenticated/);
  await on.session_shutdown({ reason: "quit" }, ctx);
  const opened: string[] = [];
  (globalThis as any)[key] = { version: 1, extensions: (host: string) => [{ name: `guard-for-${host}` }], init: () => undefined, opened: (host: string) => opened.push(host), closed: () => {} };
  await assert.rejects(add({ review: true }), /unknown or unauthenticated/);
  assert.deepEqual(opened, ["swarm"]);
  const runtime = await getRuntime(ctx);
  assert.ok(runtime.registry.snapshot().extension("guard-for-swarm"));
  delete (globalThis as any)[key];
  await on.session_shutdown({ reason: "quit" }, ctx);
});
