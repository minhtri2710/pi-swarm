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
