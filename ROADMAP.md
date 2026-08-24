# Roadmap

`faststream-azure-servicebus` is a pre-alpha Azure Service Bus broker for
FastStream. The current baseline supports queue and topic-subscription consumers,
queue and topic publishing, batch publishing, message settlement, concurrent
handlers, lock renewal, replies, broker request/reply, AsyncAPI subscriber schemas,
and connected tests against the Service Bus emulator.

## M3: Complete the broker surface and routers (complete)

- [x] Add a user-facing `ServiceBusPublisher` for direct calls and handler decorators.
- [x] Add publisher AsyncAPI specifications.
- [x] Add `ServiceBusRouter`, `ServiceBusRoute`, and delayed publisher arguments.
- [x] Support nested-router prefixes, dependencies, middleware, codecs, persistence,
  and schema inclusion.
- [x] Validate that only Service Bus routers can be included.
- [x] Define and test how AMQP `VALUE` and `SEQUENCE` bodies are decoded.
- [x] Validate mutually exclusive authentication modes.
- [x] Export the complete public API.

Acceptance criteria: queue and topic endpoints work directly and through nested
routers, including prefixes, publisher decorators, and AsyncAPI generation.

## M4: Add `TestServiceBusBroker` (complete)

- [x] Extend FastStream's in-memory `TestBroker` infrastructure.
- [x] Add a patched received-message type with observable settlement state.
- [x] Implement in-memory queue routing and topic fan-out to subscriptions.
- [x] Support handler replies, publisher decorators, and multiple test brokers.
- [x] Add fast unit coverage for validation, parsing, settlement, sender caching,
  batch rollover, lifecycle, and health checks.

Acceptance criteria: application handlers can be tested without Docker or an
Azure namespace, and the offline suite satisfies the repository's coverage gate.

## M5: Conformance suite and emulator CI (complete)

- [x] Split fast offline tests from connected emulator tests.
- [x] Cover dependencies, middleware, custom parsers, decoders, codecs, responses,
  routers, AsyncAPI, `get_one`, iteration, and publisher decorators.
- [x] Exercise every acknowledgement policy without unexpected error logs.
- [x] Cover lock renewal, concurrent shutdown, receive recovery, oversized batches,
  time-to-live, scheduling, and sender reconnection.
- [x] Keep the supported-Python CI matrix and the FastStream-main nightly job.
- [x] Pin emulator infrastructure versions for reproducible builds.

Acceptance criteria: static checks, offline tests, connected tests, coverage, and
nightly FastStream compatibility all pass.

## M6: Add broker request/reply (complete)

- [x] Add a broker `request()` API with an explicitly configured reply queue.
- [x] Set and preserve `message_id`, `correlation_id`, and `reply_to` properties.
- [x] Route concurrent outstanding requests to the correct waiter.
- [x] Define timeout, cancellation, late-reply, shutdown, and connection-recovery
  behavior without leaking receivers or background tasks.
- [x] Support queue and topic request destinations without requiring sessions.
- [x] Provide equivalent in-memory behavior through `TestServiceBusBroker`.
- [x] Cover the protocol with offline and connected emulator tests.

Acceptance criteria: applications can send concurrent requests and receive the
matching replies through both the real broker and `TestServiceBusBroker`, with
deterministic cleanup on success, failure, timeout, and shutdown.

## M7: Documentation and PyPI release

- Add a documentation site covering installation, authentication, queues, topics,
  routers, publishers, settlement, testing, emulator setup, and limitations.
- Add a changelog and a compatibility policy for FastStream private APIs.
- Verify wheel and source-distribution contents and imports.
- Rehearse publishing on TestPyPI.
- Validate that the release tag and built package version match.
- Gate trusted publishing on successful CI and smoke tests.
- Publish an explicitly alpha release.

Acceptance criteria: a new user can install the package, follow the documented
quick start, test an application offline, and understand the supported feature set.

## Explicitly deferred

- Session-enabled consumers
- Deferred-message retrieval and dead-letter queue consumption
- FastAPI integration
- OpenTelemetry and Prometheus providers
