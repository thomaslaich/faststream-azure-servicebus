from __future__ import annotations

import asyncio
import os
from pprint import pprint
from typing import TYPE_CHECKING, Any, cast

import msgpack

from faststream_azure_servicebus import ServiceBusBroker
from faststream_azure_servicebus.annotations import (
    ServiceBusMessage,  # noqa: TC001 -- FastStream resolves this annotation at runtime
)

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto
    from faststream.message import StreamMessage
    from faststream.types import DecodedMessage, SendableMessage

EMULATOR_CONNECTION_STRING = (
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"  # pragma: allowlist secret
)
QUEUE = "example-messagepack"
CONTENT_TYPE = "application/msgpack"


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


class MessagePackCodec:
    async def encode(
        self,
        msg: SendableMessage,
        serializer: SerializerProto | None = None,
    ) -> tuple[bytes, str | None]:
        # A codec owns the wire format and content type. This format does not
        # need FastDepends' object serializer, so it is intentionally unused.
        _ = serializer
        body = cast("bytes", msgpack.packb(msg, use_bin_type=True))
        return body, CONTENT_TYPE

    async def decode(self, msg: StreamMessage[Any]) -> DecodedMessage:
        if msg.content_type != CONTENT_TYPE:
            error = f"Expected {CONTENT_TYPE}, got {msg.content_type!r}"
            raise ValueError(error)

        return cast("DecodedMessage", msgpack.unpackb(msg.body, raw=False))


async def main() -> None:
    connection_string = os.environ.get(
        "SERVICEBUS_CONNECTION_STRING",
        EMULATOR_CONNECTION_STRING,
    )
    broker = ServiceBusBroker(connection_string, codec=MessagePackCodec())
    consumed = asyncio.Event()
    payload: SendableMessage = {"id": 1, "message": "hello from MessagePack"}

    @broker.subscriber(QUEUE)
    async def handle_message(
        body: dict[str, object],
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
        await broker.publish(payload, queue=QUEUE)
        await asyncio.wait_for(consumed.wait(), timeout=15)
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
