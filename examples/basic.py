from __future__ import annotations

import asyncio
import os
from pprint import pprint
from typing import TYPE_CHECKING
from uuid import uuid4

from faststream_azure_servicebus import ServiceBusBroker
from faststream_azure_servicebus.annotations import (
    ServiceBusMessage,  # noqa: TC001 -- FastStream resolves this annotation at runtime
)

if TYPE_CHECKING:
    from faststream.types import SendableMessage

EMULATOR_CONNECTION_STRING = (
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"  # pragma: allowlist secret
)
QUEUE = "example-queue"


def print_wire_message(msg: ServiceBusMessage) -> None:
    pprint(  # noqa: T203 -- intentionally show the complete wire message
        {
            "body": msg.body,
            "content_type": msg.content_type,
            "message_id": msg.message_id,
            "correlation_id": msg.correlation_id,
            "reply_to": msg.reply_to,
            "headers": msg.headers,
        },
        sort_dicts=False,
    )


async def main() -> None:
    connection_string = os.environ.get(
        "SERVICEBUS_CONNECTION_STRING",
        EMULATOR_CONNECTION_STRING,
    )
    broker = ServiceBusBroker(connection_string)
    publisher = broker.publisher(queue=QUEUE)
    consumed = asyncio.Event()
    payload: SendableMessage = {
        "id": uuid4().hex[:8],
        "message": "hello from FastStream",
    }

    @broker.subscriber(QUEUE)
    async def handle_message(
        body: dict[str, str],
        msg: ServiceBusMessage,
    ) -> None:
        print("wire message:")
        print_wire_message(msg)
        print(f"received: {body}")
        if body == payload:
            consumed.set()

    try:
        await broker.start()
        print(f"publishing: {payload}")
        await publisher.publish(payload)
        await asyncio.wait_for(consumed.wait(), timeout=15)
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
