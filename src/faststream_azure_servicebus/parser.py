from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Optional

from azure.servicebus import ServiceBusMessage as AzureServiceBusMessage
from azure.servicebus.amqp import AmqpMessageBodyType
from faststream._internal._compat import json_dumps
from faststream._internal.parser import DefaultCodec
from faststream.message import decode_message, gen_cor_id

from faststream_azure_servicebus.message import ServiceBusMessage

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime, timedelta

    from azure.servicebus import ServiceBusReceivedMessage
    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.basic_types import DecodedMessage, SendableMessage
    from faststream._internal.parser import CodecProto
    from faststream.message import StreamMessage


def extract_body(message: "ServiceBusReceivedMessage") -> bytes:
    """Normalise a received AMQP body to bytes.

    For the usual `DATA` body type the SDK hands back a generator of byte
    chunks rather than a `bytes` object, so it has to be joined. `VALUE` and
    `SEQUENCE` bodies can be anything AMQP can express.
    """
    body: Any = message.body
    body_type = getattr(message, "body_type", None)

    if body is None:
        return b""

    if body_type is AmqpMessageBodyType.SEQUENCE:
        # Preserve AMQP sequence section boundaries. The SDK exposes these as a
        # generator of lists; concatenating their string forms is lossy.
        return json_dumps(list(body))

    if body_type is AmqpMessageBodyType.VALUE:
        if isinstance(body, bytes):
            return body
        if isinstance(body, str):
            return body.encode()
        return json_dumps(body)

    if isinstance(body, bytes):
        return body

    if isinstance(body, str):
        return body.encode()

    if isinstance(body, Mapping):
        return json_dumps(body)

    if isinstance(body, Iterable):
        return b"".join(
            chunk if isinstance(chunk, bytes) else str(chunk).encode() for chunk in body
        )

    return str(body).encode()


def normalize_headers(properties: Mapping[Any, Any] | None) -> dict[str, Any]:
    """Decode AMQP application properties into str-keyed headers.

    The SDK returns application property keys — and often values — as bytes.
    FastStream's `Header` extraction works on `str`, so decode both.
    """
    if not properties:
        return {}

    headers: dict[str, Any] = {}
    for raw_key, raw_value in properties.items():
        key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)

        if isinstance(raw_value, bytes):
            try:
                headers[key] = raw_value.decode()
            except UnicodeDecodeError:
                headers[key] = raw_value
        else:
            headers[key] = raw_value

    return headers


class ServiceBusParser:
    """Translates between Service Bus messages and FastStream messages.

    `attach` lets a subscriber hand each parsed message the receiver holding its
    lock, which is what makes settlement possible. The producer leaves it unset:
    messages it parses (for `request`/reply) were never locked.
    """

    def __init__(
        self,
        attach: Optional["Callable[[ServiceBusMessage], None]"] = None,
    ) -> None:
        self._attach = attach

    async def parse_message(
        self,
        message: "ServiceBusReceivedMessage",
    ) -> "StreamMessage[ServiceBusReceivedMessage]":
        headers = normalize_headers(message.application_properties)

        parsed = ServiceBusMessage(
            raw_message=message,
            body=extract_body(message),
            headers=headers,
            reply_to=message.reply_to or "",
            content_type=message.content_type,
            message_id=str(message.message_id) if message.message_id else gen_cor_id(),
            correlation_id=str(message.correlation_id)
            if message.correlation_id
            else gen_cor_id(),
        )

        if self._attach is not None:
            self._attach(parsed)

        return parsed

    async def decode_message(
        self,
        msg: "StreamMessage[ServiceBusReceivedMessage]",
    ) -> "DecodedMessage":
        return decode_message(msg)

    @staticmethod
    async def encode_message(
        message: "SendableMessage",
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        reply_to: str = "",
        subject: str | None = None,
        session_id: str | None = None,
        partition_key: str | None = None,
        time_to_live: Optional["timedelta"] = None,
        scheduled_enqueue_time: Optional["datetime"] = None,
        serializer: Optional["SerializerProto"] = None,
        codec: Optional["CodecProto"] = None,
    ) -> AzureServiceBusMessage:
        """Build an outgoing Service Bus message."""
        if isinstance(message, AzureServiceBusMessage):
            properties = dict(message.application_properties or {})
            properties.update(headers or {})
            message.application_properties = properties
            return message

        body, content_type = await (codec or DefaultCodec()).encode(message, serializer)

        # Copy per message: the SDK keeps a reference, and one batch's messages
        # must not share a mutable properties dict.
        properties: dict[str | bytes, Any] = {}
        properties.update(headers or {})

        return AzureServiceBusMessage(
            body,
            content_type=content_type,
            application_properties=properties,
            correlation_id=correlation_id or gen_cor_id(),
            message_id=message_id,
            reply_to=reply_to or None,
            subject=subject,
            session_id=session_id,
            partition_key=partition_key,
            time_to_live=time_to_live,
            scheduled_enqueue_time_utc=scheduled_enqueue_time,
        )
