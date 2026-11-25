export type BoardMessage = { seq: number; id: string; workerId: string; channel: string; text: string; mentions: string[]; timestamp: string };
export type WorkerSubmission = { submissionId: number; status: "working" | "waiting" | "stopped" | "failed" };
export function setWorkerSubmission(worker: WorkerSubmission, submissionId: number) {
  worker.submissionId = submissionId;
  worker.status = "working";
}
export function settleWorkerSubmission(worker: WorkerSubmission, submissionId: number, status: "waiting" | "failed" = "waiting") {
  if (worker.submissionId === submissionId && worker.status === "working") worker.status = status;
}
export function selectMessages(messages: readonly BoardMessage[], channel?: string, since = 0, name?: string): BoardMessage[] {
  return messages.filter((row) => row.seq > since && (!channel || row.channel === channel) && (!name || row.workerId === name || row.mentions.includes(name.toLowerCase()) || row.mentions.includes("all")));
}
export function mentionsIn(text: string): string[] {
  return [...new Set((text.match(/@[a-z0-9_-]+|@all/gi) ?? []).map((value) => value.slice(1).toLowerCase()))];
}
export function isMentioned(message: BoardMessage, name: string): boolean {
  const normalized = name.toLowerCase();
  return message.mentions.includes("all") || message.mentions.includes(normalized);
}
