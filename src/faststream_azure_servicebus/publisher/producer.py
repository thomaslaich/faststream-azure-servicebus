from typing import TYPE_CHECKING, Any, Optional

import anyio
from azure.servicebus import ServiceBusMessageBatch
from azure.servicebus.exceptions import MessageSizeExceededError
from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import DefaultCodec
from faststream._internal.producer import ProducerProto
from faststream.exceptions import IncorrectState, SetupError
from typing_extensions import override

from faststream_azure_servicebus.parser import ServiceBusParser
from faststream_azure_servicebus.publisher.reply import ServiceBusReplyReceiver
from faststream_azure_servicebus.response import ServiceBusPublishCommand

if TYPE_CHECKING:
    from azure.servicebus import (
        ServiceBusMessage as AzureServiceBusMessage,
        ServiceBusReceivedMessage,
    )
    from azure.servicebus.aio import ServiceBusSender
    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import CustomCallable

    from faststream_azure_servicebus.configs.state import ServiceBusConnectionState


REQUEST_NOT_SUPPORTED = (
    "ServiceBusPublisher doesn't support `request`; use ServiceBusBroker.request "
    "with a configured reply_queue."
)
REPLY_QUEUE_REQUIRED = (
    "Configure ServiceBusBroker(reply_queue=...) before calling request()."
)
BROKER_NOT_CONNECTED = "Request/reply requires a connected ServiceBusBroker."


class ServiceBusProducer(ProducerProto[ServiceBusPublishCommand]):
    """Sends messages to Service Bus queues and topics."""

    _parser: "ParserComposition"
    _decoder: "ParserComposition"

    def __init__(
        self,
        *,
        connection: "ServiceBusConnectionState",
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
        reply_queue: str | None = None,
        serializer: Optional["SerializerProto"] = None,
        codec: Optional["CodecProto"] = None,
    ) -> None:
        self._connection = connection
        self.reply_queue = reply_queue
        self._reply_receiver = (
            ServiceBusReplyReceiver(connection, reply_queue) if reply_queue else None
        )
        self._connected = False

        default = ServiceBusParser()
        self._parser = ParserComposition(parser, default.parse_message)  # pyright: ignore[reportIncompatibleMethodOverride]
        self._decoder = ParserComposition(decoder, default.decode_message)  # pyright: ignore[reportIncompatibleMethodOverride]

        self.serializer = serializer
        self.codec: CodecProto = codec or DefaultCodec()

    def connect(
        self,
        serializer: Optional["SerializerProto"] = None,
        codec: Optional["CodecProto"] = None,
    ) -> None:
        self.serializer = serializer
        if codec is not None:
            self.codec = codec
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False
        if self._reply_receiver is not None:
            await self._reply_receiver.stop()

    @override
    async def publish(self, cmd: "ServiceBusPublishCommand") -> None:
        message = await self._encode(cmd, cmd.body)

        async with self._connection.sender(
            cmd.destination_type,
            cmd.destination,
        ) as sender:
            await sender.send_messages(message)

    @override
    async def publish_batch(self, cmd: "ServiceBusPublishCommand") -> None:
        bodies = cmd.batch_bodies
        if not bodies:
            return

        messages = [await self._encode(cmd, body) for body in bodies]

        async with self._connection.sender(
            cmd.destination_type,
            cmd.destination,
        ) as sender:
            for batch in await self._pack(sender, messages):
                await sender.send_messages(batch)

    @override
    async def request(
        self,
        cmd: "ServiceBusPublishCommand",
    ) -> "ServiceBusReceivedMessage":
        if self._reply_receiver is None:
            raise SetupError(REPLY_QUEUE_REQUIRED)
        if not self._connected:
            raise IncorrectState(BROKER_NOT_CONNECTED)

        correlation_id = cmd.correlation_id
        assert correlation_id is not None

        await self._reply_receiver.start()
        future = self._reply_receiver.register(correlation_id)

        try:
            with anyio.fail_after(cmd.timeout):
                await self.publish(cmd)
                return await future
        finally:
            self._reply_receiver.unregister(correlation_id)
            if future.done() and not future.cancelled():
                # If publishing failed while shutdown also failed the waiter,
                # retrieve that exception so the abandoned future stays quiet.
                future.exception()

    async def _encode(
        self,
        cmd: "ServiceBusPublishCommand",
        body: Any,
    ) -> "AzureServiceBusMessage":
        return await ServiceBusParser.encode_message(
            body,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            message_id=cmd.message_id,
            reply_to=cmd.reply_to,
            subject=cmd.subject,
            session_id=cmd.session_id,
            partition_key=cmd.partition_key,
            time_to_live=cmd.time_to_live,
            scheduled_enqueue_time=cmd.scheduled_enqueue_time,
            serializer=self.serializer,
            codec=self.codec,
        )

    @staticmethod
    async def _pack(
        sender: "ServiceBusSender",
        messages: list["AzureServiceBusMessage"],
    ) -> list[ServiceBusMessageBatch]:
        """Split messages into batches that fit the service's frame size.

        Service Bus caps a batch at 256 KB (1 MB on premium), and the SDK only
        reports the overflow by raising when a message won't fit.
        """
        batches: list[ServiceBusMessageBatch] = []
        batch = await sender.create_message_batch()

        for message in messages:
            try:
                batch.add_message(message)
            except (MessageSizeExceededError, ValueError):  # noqa: PERF203 — the SDK reports a full batch only by raising
                if len(batch) == 0:
                    # A single message that cannot fit an empty batch is a real
                    # error — the caller has to shrink it.
                    raise

                batches.append(batch)
                batch = await sender.create_message_batch()
                batch.add_message(message)

        if len(batch):
            batches.append(batch)

        return batches
