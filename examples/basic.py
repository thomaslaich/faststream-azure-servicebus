import asyncio
import os
from uuid import uuid4

from faststream_azure_servicebus import ServiceBusBroker

EMULATOR_CONNECTION_STRING = (
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"  # pragma: allowlist secret
)
QUEUE = "example-queue"


async def main() -> None:
    connection_string = os.environ.get(
        "SERVICEBUS_CONNECTION_STRING",
        EMULATOR_CONNECTION_STRING,
    )
    broker = ServiceBusBroker(connection_string)
    publisher = broker.publisher(queue=QUEUE)
    consumed = asyncio.Event()
    payload = {"id": uuid4().hex[:8], "message": "hello from FastStream"}

    @broker.subscriber(QUEUE)
    async def handle_message(body: dict[str, str]) -> None:
        print(f"received: {body}")
        if body == payload:
            consumed.set()

    try:
        await broker.start()
        await publisher.publish(payload)
        await asyncio.wait_for(consumed.wait(), timeout=15)
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
