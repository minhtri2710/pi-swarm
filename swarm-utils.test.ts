import assert from "node:assert/strict";
import test from "node:test";
import { mentionsIn, selectMessages, settleWorkerSubmission, setWorkerSubmission } from "./swarm-utils.ts";

const rows = [
  { seq: 1, id: "a", workerId: "lead", channel: "main", text: "hello", mentions: ["worker"], timestamp: "now" },
  { seq: 2, id: "b", workerId: "worker", channel: "other", text: "own", mentions: [], timestamp: "now" },
  { seq: 3, id: "c", workerId: "lead", channel: "main", text: "all", mentions: ["all"], timestamp: "now" },
];

test("mentions normalize and deduplicate worker names", () => {
  assert.deepEqual(mentionsIn("Ping @Coder and @all; again @coder"), ["coder", "all"]);
});

test("a late submission watcher cannot mark a newer worker task as waiting", () => {
  const worker = { submissionId: 1, status: "working" as const };
  setWorkerSubmission(worker, 2);
  settleWorkerSubmission(worker, 1);
  assert.deepEqual(worker, { submissionId: 2, status: "working" });
  settleWorkerSubmission(worker, 2);
  assert.equal(worker.status, "waiting");
  setWorkerSubmission(worker, 3);
  settleWorkerSubmission(worker, 3, "failed");
  assert.equal(worker.status, "failed");
});

test("durable board selection applies cursor, channel, and worker mention filters", () => {
  assert.deepEqual(selectMessages(rows, "main", 1, "worker"), [rows[2]]);
  assert.deepEqual(selectMessages(rows, undefined, 0), rows);
});
