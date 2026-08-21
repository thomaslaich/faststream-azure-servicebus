from typing import TYPE_CHECKING, Any, NoReturn, Optional

from azure.servicebus.exceptions import MessageSizeExceededError
from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import DefaultCodec
from faststream._internal.producer import ProducerProto
from faststream.exceptions import FeatureNotSupportedException
from typing_extensions import override

from faststream_azure_servicebus.parser import ServiceBusParser
from faststream_azure_servicebus.response import ServiceBusPublishCommand

if TYPE_CHECKING:
    from azure.servicebus import ServiceBusMessage as AzureServiceBusMessage
    from azure.servicebus.aio import ServiceBusSender
    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import CustomCallable

    from faststream_azure_servicebus.configs.state import ServiceBusConnectionState


REQUEST_NOT_SUPPORTED = (
    "ServiceBusBroker doesn't support `request` yet. Request/reply over Service Bus "
    "needs a session-enabled reply entity, which arrives with session support."
)


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
        serializer: Optional["SerializerProto"] = None,
        codec: Optional["CodecProto"] = None,
    ) -> None:
        self._connection = connection

        default = ServiceBusParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)

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
    async def request(self, cmd: "ServiceBusPublishCommand") -> NoReturn:
        raise FeatureNotSupportedException(REQUEST_NOT_SUPPORTED)

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
    ) -> list[Any]:
        """Split messages into batches that fit the service's frame size.

        Service Bus caps a batch at 256 KB (1 MB on premium), and the SDK only
        reports the overflow by raising when a message won't fit.
        """
        batches = []
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
