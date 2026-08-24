# faststream-azure-servicebus

An [Azure Service Bus](https://learn.microsoft.com/azure/service-bus-messaging/) broker for
[FastStream](https://github.com/ag2ai/faststream).

## Why?

https://github.com/ag2ai/faststream/issues/822

## Quickstart

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

The repository includes runnable
[`publish/consume`](examples/basic.py) and
[`request/reply`](examples/request_reply.py), and
[`MessagePack`](examples/messagepack.py) examples for the local emulator.

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
- AsyncAPI schemas
- Offline application tests with `TestServiceBusBroker`

Planned, not yet implemented: sessions, deferred-message retrieval, dead-letter queue
consumption, the FastAPI plugin, and the OpenTelemetry and Prometheus providers.

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

The emulator, tests, and example form a process graph in
[`process-compose.yaml`](process-compose.yaml). The test and example processes
depend on the emulator's HTTP readiness probe and tear the Compose stack down
when they exit. `just up` and `just down` instead manage a persistent background
instance; `just logs` attaches to its TUI and container logs. Run
`process-compose up` directly for the full foreground TUI. The runnable example
sources are in [`examples/basic.py`](examples/basic.py) and
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
