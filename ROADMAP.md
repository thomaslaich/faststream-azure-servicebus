# Roadmap

`faststream-azure-servicebus` is a pre-alpha Azure Service Bus broker for
FastStream. The current baseline supports queue and topic-subscription consumers,
queue and topic publishing, batch publishing, message settlement, concurrent
handlers, lock renewal, replies, AsyncAPI subscriber schemas, and connected tests
against the Service Bus emulator.

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

## M4: Add `TestServiceBusBroker`

- Extend FastStream's in-memory `TestBroker` infrastructure.
- Add a patched received-message type with observable settlement state.
- Implement in-memory queue routing and topic fan-out to subscriptions.
- Support handler replies, publisher decorators, and multiple test brokers.
- Add fast unit coverage for validation, parsing, settlement, sender caching,
  batch rollover, lifecycle, and health checks.

Acceptance criteria: application handlers can be tested without Docker or an
Azure namespace, and the offline suite satisfies the repository's coverage gate.

## M5: Conformance suite and emulator CI

- Split fast offline tests from connected emulator tests.
- Cover dependencies, middleware, custom parsers, decoders, codecs, responses,
  routers, AsyncAPI, `get_one`, iteration, and publisher decorators.
- Exercise every acknowledgement policy without unexpected error logs.
- Cover lock renewal, concurrent shutdown, receive recovery, oversized batches,
  time-to-live, scheduling, and sender reconnection.
- Keep the supported-Python CI matrix and the FastStream-main nightly job.
- Pin emulator infrastructure versions for reproducible builds.

Acceptance criteria: static checks, offline tests, connected tests, coverage, and
nightly FastStream compatibility all pass.

## M6: Documentation and PyPI release

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

- Session-enabled consumers and request/reply
- Deferred-message retrieval and dead-letter queue consumption
- FastAPI integration
- OpenTelemetry and Prometheus providers
