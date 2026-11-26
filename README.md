# pi-swarm

Peer Pi workers are independent `pi-durable` conversations. Worker records, transcripts, and the shared message board are stored in a session-scoped SQLite database.

## Install

```sh
cd ~/Work/pi-swarm && npm install --legacy-peer-deps
pi install ~/Work/pi-swarm
```

Pi's local package installer does not install npm dependencies; run `npm install --legacy-peer-deps` in the package directory first. The `swarm` tool supports `models`, `add`, `status`, `model`, `stop`, `post`, `read`, `wait`, and `transcript`; `/swarm status` and `/swarm board` offer quick inspection. `add` requires a worker name, brief, and an authenticated `provider/model` (or inherits the coordinator model). Messages mentioning `@name` or `@all` are appended to the durable board and delivered as follow-up submissions to the matching workers. Every board record has a monotonic sequence number within the session database. `add` also takes `review` and `testCommand` for review and test-after-edit loops; they need the optional [pi-worker-guards](https://github.com/minhtri2710/pi-worker-guards) extension and are refused without it. With that extension installed, risky worker `bash` calls park until `/guards-approve` or `/guards-deny`.

## Limits and safety

Requires `pi-durable`, the Node SQLite backend, and an authenticated model. A single session supports at most eight workers; the board retains the latest 10,000 messages. Worker tool calls run under the host process account and project permissions; do not treat worker or board text as trusted instructions. Persisted session databases are kept in the Pi session directory with owner-only permissions; only one process may own a session database at a time. `pi --no-session` uses in-memory storage, which does not survive process exit. Tests use `node --experimental-strip-types --test swarm-utils.test.ts durable-runtime.test.ts`.
