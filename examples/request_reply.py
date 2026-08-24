import asyncio
import os
from pprint import pprint
from typing import TYPE_CHECKING

from faststream_azure_servicebus import ServiceBusBroker
from faststream_azure_servicebus.annotations import ServiceBusMessage

if TYPE_CHECKING:
    from faststream.types import SendableMessage

EMULATOR_CONNECTION_STRING = (
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"  # pragma: allowlist secret
)
REQUEST_QUEUE = "example-requests"
REPLY_QUEUE = "example-replies"


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

    @broker.subscriber(REQUEST_QUEUE)
    async def double(
        body: dict[str, int],
        msg: ServiceBusMessage,
    ) -> dict[str, int]:
        print("request wire message:")
        print_wire_message(msg)
        print(f"received: {body}. answering...")
        return {"answer": body["value"] * 2}

    try:
        await broker.start()
        payload: SendableMessage = {"value": 21}
        print(f"sending: {payload}")
        response = await broker.request(
            payload,
            queue=REQUEST_QUEUE,
            reply_to=REPLY_QUEUE,
            timeout=30,
        )
        print("reply wire message:")
        print_wire_message(response)
        print(f"reply: {await response.decode()}")
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
