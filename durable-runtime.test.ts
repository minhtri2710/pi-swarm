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
    const context = await (await harness.conversation(worker.id, BACKGROUND_CONTEXT))!.context(BACKGROUND_CONTEXT);
    assert.match(JSON.stringify(context.messages), /go ahead/);
    assert.equal((await snapshot(ref.value)).sequence, 1);
    await harness.close(BACKGROUND_CONTEXT);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
