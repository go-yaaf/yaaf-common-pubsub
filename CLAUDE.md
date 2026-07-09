# CLAUDE.md

The guidance for working in this repository lives in **[AGENTS.md](./AGENTS.md)** — read it first.

It covers the architecture, repository layout, build/test commands, configuration knobs, and the
invariants you must preserve (Pub/Sub v2 API, resource-name validation, subscription lifecycle,
malformed-message policy, and `Read` concurrency).

Additional references:
- `README.md` — human-facing usage documentation.
- `llms.txt` — extended usage/narrative reference.
