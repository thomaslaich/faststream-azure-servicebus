# faststream-azure-servicebus

An [Azure Service Bus](https://learn.microsoft.com/azure/service-bus-messaging/) broker for
[FastStream](https://github.com/ag2ai/faststream).

## Why?

https://github.com/ag2ai/faststream/issues/822

## Quickstart

```bash
uv add faststream-azure-servicebus
```

### Publish and consume

```python
from faststream import FastStream
from faststream_azure_servicebus import ServiceBusBroker

broker = ServiceBusBroker("Endpoint=sb://<namespace>.servicebus.windows.net/;...")
app = FastStream(broker)
orders = broker.publisher(queue="orders")


@broker.subscriber(queue="orders")
async def handle_order(body: dict) -> None:
    print(body)


@broker.subscriber(topic="events", subscription="billing")
async def handle_event(body: dict) -> None:
    print(body)


@app.after_startup
async def publish() -> None:
    await orders.publish({"id": 1})
    await broker.publish({"kind": "created"}, topic="events")
```

For passwordless Azure authentication, install the identity extra and provide only
the namespace. The broker owns the resulting `DefaultAzureCredential` and closes it
on shutdown:

```bash
uv add "faststream-azure-servicebus[identity]"
```

```python
broker = ServiceBusBroker(
    fully_qualified_namespace="<namespace>.servicebus.windows.net",
)
```

Pass `credential=` explicitly instead when its lifecycle is shared; caller-provided
credentials remain caller-owned.

### Request and reply

Request/reply requires a pre-existing reply queue owned by the caller:

```python
import asyncio

from faststream_azure_servicebus import ServiceBusBroker

broker = ServiceBusBroker("Endpoint=sb://<namespace>.servicebus.windows.net/;...")


@broker.subscriber(queue="commands")
async def handle_command(body: dict[str, int]) -> dict[str, int]:
    return {"answer": body["value"] * 2}


async def main() -> None:
    try:
        await broker.start()
        response = await broker.request(
            {"value": 21},
            queue="commands",
            reply_to="rpc-replies",
            timeout=10,
        )
        print(await response.decode())  # {'answer': 42}
    finally:
        await broker.stop()


asyncio.run(main())
```

### Observability

Install the OpenTelemetry extra and add the Service Bus middleware to the broker:

```bash
uv add "faststream-azure-servicebus[otel]"
```

```python
from faststream_azure_servicebus import ServiceBusBroker
from faststream_azure_servicebus.opentelemetry import ServiceBusTelemetryMiddleware

broker = ServiceBusBroker(
    "Endpoint=sb://<namespace>.servicebus.windows.net/;...",
    middlewares=(ServiceBusTelemetryMiddleware(),),
)
```

The middleware uses the global OpenTelemetry tracer and meter providers by default.
It propagates W3C trace context and baggage in Service Bus application properties and
emits FastStream's standard `messaging.publish.*` and `messaging.process.*` spans and
metrics with `messaging.system=servicebus`. Explicit tracer, meter, or meter providers
can be passed to the middleware instead.

For native Prometheus metrics, install the Prometheus extra and provide a collector
registry:

```bash
uv add "faststream-azure-servicebus[prometheus]"
```

```python
from prometheus_client import REGISTRY

from faststream_azure_servicebus import ServiceBusBroker
from faststream_azure_servicebus.prometheus import ServiceBusPrometheusMiddleware

broker = ServiceBusBroker(
    "Endpoint=sb://<namespace>.servicebus.windows.net/;...",
    middlewares=(
        ServiceBusPrometheusMiddleware(registry=REGISTRY, app_name="orders"),
    ),
)
```

This emits FastStream's standard publish, receive, processing, duration, size,
in-process, and exception metrics. Destination and status labels distinguish queues,
topic subscriptions, publish success or failure, and ack/nack/reject outcomes.

The repository includes runnable
[`publish/consume with OpenTelemetry`](examples/basic.py) and
[`request/reply`](examples/request_reply.py), and
[`MessagePack`](examples/messagepack.py) examples for the local emulator.
`just example` also starts a local
[`grafana/otel-lgtm`](https://hub.docker.com/r/grafana/otel-lgtm) stack and exports
the example's traces and metrics over OTLP. Open Grafana at
<http://localhost:3000/explore> after the example starts, then press `Ctrl+C` to
stop the example and tear down its local services.

## Features

Supported today:

- Queue publishers and subscribers
- Topic publishers, topic-subscription subscribers
- Handler replies and correlated broker request/reply
- Concurrent handlers and concurrent request waiters
- Peek-lock settlement mapped onto FastStream's `AckPolicy`
  (`complete` / `abandon` / `dead-letter`)
- Background message-lock renewal for long-running handlers
- Batch publishing
- Message time-to-live and scheduled enqueueing
- Routers, publisher decorators, middleware, dependencies, and custom codecs
- OpenTelemetry tracing, context propagation, and messaging metrics
- Native Prometheus messaging metrics
- AsyncAPI schemas
- Offline application tests with `TestServiceBusBroker`

Planned, not yet implemented: sessions, deferred-message retrieval, dead-letter queue
consumption, and the FastAPI plugin.

## Development

The recommended development environment uses [Nix](https://nixos.org/) and
[devenv](https://devenv.sh/). It provisions Python, uv, just, treefmt, and all
formatters from the checked-in configuration. Docker is required for the connected
test suite.

1. Install Nix (recommended: [Determinate Nix](https://determinate.systems/nix/))
   and devenv.
2. Optionally install [direnv](https://direnv.net/) and run `direnv allow` to
   activate the environment automatically. Otherwise, run `devenv shell`.
3. Use the `just` recipes from inside the environment:

```bash
git clone https://github.com/thomaslaich/faststream-azure-servicebus
cd faststream-azure-servicebus
direnv allow                 # or: devenv shell

just up                      # Service Bus emulator + its SQL Edge backing store
just example                 # publish and consume one message
just example-request-reply   # send a request and receive its reply
just example-messagepack     # publish and consume MessagePack
just test
just check                   # treefmt, ruff check, ty
```

`just --list` shows the rest.

Connected tests use Testcontainers to start one Service Bus emulator and SQL Edge
stack per pytest session. With xdist, the controller shares that stack with both
workers; pytest waits for the configured entity pool and removes the containers
when the session ends. Set `SERVICEBUS_CONNECTION_STRING` to test against an
existing dedicated namespace instead. The `just` recipes pass the required
`--servicebus-emulator` option when running with xdist.

The examples form a process graph in [`process-compose.yaml`](process-compose.yaml).
They depend on the emulator's HTTP readiness probe and tear the Compose stack down
when they exit. `just up` and `just down` instead manage a persistent background
instance; `just logs` attaches to its TUI and container logs. Run `process-compose
up` directly for the full foreground TUI. The runnable example sources are in
[`examples/basic.py`](examples/basic.py) and
[`examples/request_reply.py`](examples/request_reply.py). The custom codec example is
in [`examples/messagepack.py`](examples/messagepack.py).

The nightly workflow installs FastStream from upstream `main` so private-API drift
shows up early without vendoring FastStream in this repository.

The emulator cannot create entities at runtime and caps a namespace at 50 of them, so
tests lease names from a fixed pool declared in `tests/infra/servicebus-config.json`.
Regenerate that file with `python tests/infra/generate_config.py` after changing the pool.

Two emulator limits shape the test setup: a namespace allows only **10 concurrent
connections** (hence `-n 2` rather than `-n auto`), and entities cannot be created at
runtime. Its SQL Edge dependency ships a native arm64 image, so don't pin
`platform: linux/amd64` — the amd64 build segfaults under emulation on Apple Silicon.
