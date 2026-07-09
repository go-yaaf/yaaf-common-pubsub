# AGENTS.md

Operational guide for AI coding assistants working in this repository. Human-facing usage docs
live in `README.md`; a broader narrative reference lives in `llms.txt`. **This file is about how to
work on the code correctly** — architecture, invariants, commands, and gotchas.

## What this is

`github.com/go-yaaf/yaaf-common-pubsub` is a thin adapter that implements the `IMessageBus`
interface from [`yaaf-common`](https://github.com/go-yaaf/yaaf-common) on top of **Google Cloud
Pub/Sub**. It is middleware: it translates the framework's messaging abstraction into Pub/Sub
admin + data-plane calls. It is a **library**, not a service — there is no `main` binary to run
(the `examples/` module is a demo).

- Language: Go (see `go.mod` for the toolchain version).
- Single real package: `gpubsub/`.
- Pub/Sub client: **`cloud.google.com/go/pubsub/v2`** (the v2 API — see "Pub/Sub v2 invariants").

## Repository layout

| Path | Purpose |
|------|---------|
| `gpubsub/pubsub_adapter.go` | Adapter struct, constructor, `Ping`/`Close`/`CloneMessageBus`, name-path helpers, validation, config helpers, `rawToMessage`/`messageToRaw`. |
| `gpubsub/pubsub_message_bus.go` | Data-plane + admin logic: `Publish`, `Subscribe`, `Unsubscribe`, `CreateProducer`/`CreateConsumer`, producer/consumer types, `getOrCreateTopic`/`getOrCreateSubscription`. |
| `gpubsub/streaming_factory.go` | `StreamingFactory(uri)` — dispatches on URI scheme (`pubsub://` → Pub/Sub, otherwise in-memory bus). |
| `gpubsub/validation_test.go` | Unit test for resource-name validation (emulator-free, runs in CI). |
| `test/` | Integration tests (`package test`) that need a running Pub/Sub emulator. |
| `examples/pubsub/` | Standalone demo module (publisher/logger/aggregator) — not imported by the library. |
| `.github/workflows/build.yml` | CI: test + publish. SHA-pinned actions, least-privilege permissions. |
| `test/docker-compose.yml`, `examples/docker-compose.yml` | Local emulator + UI. |

## Architecture & data flow

`pubSubAdapter` (in `pubsub_adapter.go`) holds the `*pubsub.Client`, the parsed `projectId`, and a
mutex-guarded map of active subscription cancel funcs.

- **Serialization is JSON** via `yaaf-common/entity` `Marshal`/`Unmarshal`. Outbound:
  `messageToRaw`. Inbound: `rawToMessage(factory, bytes)` — the `MessageFactory` produces the
  concrete type, JSON is unmarshalled into it.
- **Admin vs data plane (v2 split):**
  - Admin (create/get/delete topics & subscriptions) → `client.TopicAdminClient` /
    `client.SubscriptionAdminClient` with `pubsubpb.*` protobuf types and **fully qualified names**.
  - Data plane → `client.Publisher(name)` (publish) and `client.Subscriber(name)` (receive).
- **Resource names:** admin calls require FQNs, built by `topicPath`/`subscriptionPath`
  (`projects/{projectId}/topics/{id}` etc.). Never hand a bare id to an admin call.

## Public API (what the adapter implements)

`IMessageBus`: `Ping`, `Close`, `CloneMessageBus`, `Publish`, `Subscribe`, `Unsubscribe`,
`Push` (unsupported — returns error), `Pop` (unsupported — returns error), `CreateProducer`,
`CreateConsumer`. Plus `IMessageProducer` (`Publish`, `Close`) and `IMessageConsumer`
(`Read`, `Close`).

Entry points for callers: `NewPubSubMessageBus("pubsub://<project-id>")` or
`StreamingFactory(uri)`.

Constraints baked into this implementation:
- `Subscribe` and `CreateConsumer` accept **exactly one** topic (they error otherwise).
- `Push`/`Pop` are intentionally unsupported (Pub/Sub is not a queue); use Publish + Subscribe/Consumer.

## Build / test / lint commands

```bash
go build ./...                 # compile everything
go vet ./...                   # static checks
go test ./gpubsub/...          # unit tests (no emulator needed)
go test -race ./gpubsub/...    # with race detector (CI uses -race)
CI=true go test ./...          # full run; emulator tests skip via skipCI (see below)
go mod tidy                    # after changing imports/deps
```

- **Emulator integration tests** (`test/`) require a running emulator. Start one with
  `docker compose -f test/docker-compose.yml up`, unset `CI`, then `go test ./test/...`.
- `test/def_test.go` defines `skipCI(t)`: every emulator test calls it, and it skips when the
  `CI` env var is set. CI (`build.yml`) sets `CI=true`, so those tests do not run in the pipeline.
- Always run `go build ./... && go vet ./...` before finishing a change. Prefer adding
  emulator-free unit tests in `package gpubsub` (like `validation_test.go`) for logic that does
  not require Pub/Sub.

## Configuration (env vars, read via `yaaf-common/config`)

Read through `config.Get().Get{Bool,Int,String}ParamValueOrDefault(...)`. Defaults preserve
historical behavior.

| Key | Default | Effect |
|-----|---------|--------|
| `PUBSUB_EMULATOR_HOST` | unset | Google client env var; when set, connects to a local emulator (no auth/TLS). Must be unset in production. |
| `PUBSUB_AUTO_CREATE_RESOURCES` | `true` | `false` → missing topics/subscriptions error instead of being created on demand. Constant: `cfgAutoCreateResources`. |
| `PUBSUB_MAX_MESSAGE_BYTES` | `10485760` (10 MiB) | Inbound payload cap in `rawToMessage`; `<= 0` disables. Constant: `cfgMaxMessageBytes`. |
| yaaf-common keys | — | `EnableMessageOrdering`, `PubSubAckDeadline`, `PubSubNumOfGoroutines`, `PubSubMaxOutstandingMessages`, `PubSubMaxOutstandingBytes`. |

When adding a new knob, define a `cfg*` constant near the others in `pubsub_adapter.go` and read it
through a small helper (see `autoCreateResources()`, `maxMessageBytes()`).

## Invariants an agent MUST preserve

**Pub/Sub v2 API**
- Import `cloud.google.com/go/pubsub/v2` and `.../v2/apiv1/pubsubpb`. There is **no** `Topic` /
  `Subscription` handle type and **no** `Exists()` in v2. Check existence with `GetTopic` /
  `GetSubscription` + `status.Code(err) == codes.NotFound`.
- Publish via `*pubsub.Publisher`; receive via `*pubsub.Subscriber`. Message ordering is a
  publisher/subscription setting (`Publisher.EnableMessageOrdering`, `Subscription.EnableMessageOrdering`),
  **not** a topic-config field.

**Resource-name safety**
- Every id that reaches an admin call goes through `validateResourceName` first
  (`getOrCreateTopic`, `getOrCreateSubscription`, `Unsubscribe`). The regex rejects `/` and
  path-traversal — do not bypass it or build FQNs from unvalidated input. Add validation to any
  new code path that accepts a caller-supplied topic/subscription id.

**Subscription lifecycle (no goroutine leaks)**
- `Subscribe` runs `Subscriber.Receive` in a goroutine under a **cancellable context**, registering
  its cancel func in `adapter.cancels` keyed by subscription id (pointer-identity `*subReceiver`).
- `Unsubscribe` cancels the loop **before** deleting the subscription server-side; `Close` cancels
  **all** loops before closing the client. Preserve this ordering and the mutex discipline
  (`adapter.mu` guards `adapter.cancels`). Never reintroduce `context.Background()` for a receive
  loop that has no way to be cancelled.

**Malformed-message policy (deliberate)**
- On unmarshal failure in `Subscribe`, the message is **Ack'd (dropped) and logged at Error with
  its id** — a chosen policy to avoid poison-message redelivery loops. Do not silently change this
  to Nack/DLQ without an explicit decision; it is intentional.

**`Read` concurrency**
- `Consumer.Read` hands the message out via a buffered channel (size 1); extra prefetched messages
  are Nack'd, not dropped. Do not share plain locals across the `Receive` callback boundary — that
  reintroduces a data race.

## Conventions

- Match the surrounding style: region banner comments (`// region ... ---`), doc comments on
  exported and most unexported funcs, `fmt.Errorf("...: %v", err)` wrapping.
- Keep serialization JSON-only (that is what the framework and consumers expect).
- Adapter methods use value receiver name `r`; producers `p`; consumers `c`.
- After editing imports/deps, run `go mod tidy`. Prefer SHA-pinned GitHub Actions in CI.
- Do not commit secrets; auth is via Google Application Default Credentials (no keys in-repo).

## Security-sensitive areas (touch with care)

- `rawToMessage` — the only untrusted-input sink (inbound message bytes). Size guard lives here.
- `validateResourceName` — the path-injection guard. Covered by `validation_test.go`.
- Auto-creation gate — prevents resource-exhaustion via attacker-influenced topic names.
- `Ping` — real connectivity probe (`ListTopics` page size 1); keep it lightweight and bounded by
  `pingTimeout`.

## Gotchas

- `Publish` (adapter-level) creates a `Publisher` per topic and **`Stop()`s them in a defer** to
  flush and release goroutines. Producers created via `CreateProducer` are long-lived — their
  `Close()` calls `Publisher.Stop()`. Do not leak publishers.
- `Subscriber.ID()` panics on a name without a `/`; always pass FQNs (the code does).
- The demo under `examples/` is a separate module — do not assume library changes are exercised by it.
- `go get -v -t ...` appears in older docs; CI uses `go mod download` + `go mod verify` instead.
