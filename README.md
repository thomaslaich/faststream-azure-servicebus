# faststream-azure-servicebus

An [Azure Service Bus](https://learn.microsoft.com/azure/service-bus-messaging/) broker for
[FastStream](https://github.com/ag2ai/faststream).

> **Status: pre-alpha.** Under active development; the public API will change.

See the [roadmap](ROADMAP.md) for the planned broker, testing, and release work.

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

## Scope

Supported in the first release line:

- Queue publishers and subscribers
- Topic publishers, topic-subscription subscribers
- Peek-lock settlement mapped onto FastStream's `AckPolicy`
  (`complete` / `abandon` / `dead-letter`)
- Background message-lock renewal for long-running handlers
- Batch publishing

Planned, not yet implemented: sessions, scheduled messages, deferral, dead-letter
queue consumption, the FastAPI plugin, and the OpenTelemetry and Prometheus providers.

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
source is in [`examples/basic.py`](examples/basic.py).

The nightly workflow installs FastStream from upstream `main` so private-API drift
shows up early without vendoring FastStream in this repository.

The emulator cannot create entities at runtime and caps a namespace at 50 of them, so
tests lease names from a fixed pool declared in `tests/infra/servicebus-config.json`.
Regenerate that file with `python tests/infra/generate_config.py` after changing the pool.

Two emulator limits shape the test setup: a namespace allows only **10 concurrent
connections** (hence `-n 2` rather than `-n auto`), and entities cannot be created at
runtime. Its SQL Edge dependency ships a native arm64 image, so don't pin
`platform: linux/amd64` — the amd64 build segfaults under emulation on Apple Silicon.

## License

MIT
