from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast, overload
from unittest.mock import MagicMock

from azure.servicebus.amqp import AmqpMessageBodyType
from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import DefaultCodec
from faststream._internal.testing.broker import EnterType, TestBroker, change_producer
from faststream.exceptions import FeatureNotSupportedException
from faststream.message import gen_cor_id

from faststream_azure_servicebus.broker import ServiceBusBroker
from faststream_azure_servicebus.parser import ServiceBusParser
from faststream_azure_servicebus.publisher.producer import REQUEST_NOT_SUPPORTED
from faststream_azure_servicebus.response import DestinationType
from faststream_azure_servicebus.schemas import QueueDestination, SubscriptionDestination

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.parser import CodecProto

    from faststream_azure_servicebus.publisher import ServiceBusPublisher
    from faststream_azure_servicebus.response import ServiceBusPublishCommand
    from faststream_azure_servicebus.subscriber import ServiceBusSubscriber

__all__ = ("PatchedMessage", "TestServiceBusBroker")


@dataclass
class PatchedMessage:
    """SDK-shaped received message with observable settlement state."""

    body: Iterable[bytes]
    application_properties: dict[str, Any] = field(default_factory=dict)
    content_type: str | None = None
    correlation_id: str | None = None
    message_id: str | None = None
    reply_to: str | None = None
    subject: str | None = None
    delivery_count: int = 0
    body_type: AmqpMessageBodyType = AmqpMessageBodyType.DATA

    completed: bool = False
    abandoned: bool = False
    dead_lettered: bool = False
    deferred: bool = False
    lock_renewed: bool = False
    dead_letter_reason: str | None = None
    dead_letter_description: str | None = None


class PatchedReceiver:
    async def complete_message(self, message: PatchedMessage) -> None:
        message.completed = True

    async def abandon_message(self, message: PatchedMessage) -> None:
        message.abandoned = True

    async def dead_letter_message(
        self,
        message: PatchedMessage,
        *,
        reason: str | None = None,
        error_description: str | None = None,
    ) -> None:
        message.dead_lettered = True
        message.dead_letter_reason = reason
        message.dead_letter_description = error_description

    async def defer_message(self, message: PatchedMessage) -> None:
        message.deferred = True

    async def renew_message_lock(self, message: PatchedMessage) -> None:
        message.lock_renewed = True


class TestServiceBusBroker(
    TestBroker[ServiceBusBroker, EnterType],  # ty: ignore[invalid-type-arguments]
):
    """Run Service Bus applications without an Azure namespace or emulator."""

    @overload
    def __init__(
        self: "TestServiceBusBroker[ServiceBusBroker]",
        broker: ServiceBusBroker,
        /,
        *,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: "TestServiceBusBroker[tuple[ServiceBusBroker, ...]]",
        *brokers: ServiceBusBroker,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None: ...

    def __init__(
        self,
        *brokers: ServiceBusBroker,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None:
        super().__init__(*brokers, with_real=with_real, connect_only=connect_only)

    @contextmanager
    def _patch_producer(
        self,
        broker: ServiceBusBroker,
    ) -> Generator[None, None, None]:
        with change_producer(
            broker.config.broker_config,
            FakeProducer(broker, self.brokers),
        ):
            yield

    @staticmethod
    async def _fake_connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        broker: ServiceBusBroker,
        *args: Any,
        **kwargs: Any,
    ) -> MagicMock:  # ty: ignore[invalid-method-override]
        return MagicMock()

    def create_publisher_fake_subscriber(
        self,
        broker: ServiceBusBroker,
        publisher: "ServiceBusPublisher",
    ) -> tuple["ServiceBusSubscriber", bool]:
        matching = list(
            _matching_subscribers(
                self.brokers, publisher.destination_type, publisher.destination
            )
        )
        if matching:
            return matching[0], True

        if publisher.destination_type is DestinationType.Queue:
            sub = broker.subscriber(queue=publisher.destination, persistent=False)
        else:
            sub = broker.subscriber(
                topic=publisher.destination,
                subscription="__test__",
                persistent=False,
            )
        return sub, False


class FakeProducer:
    def __init__(
        self, broker: ServiceBusBroker, brokers: Sequence[ServiceBusBroker]
    ) -> None:
        self.broker = broker
        self.brokers = brokers
        parser = ServiceBusParser()
        self._parser = ParserComposition(broker._parser, parser.parse_message)
        self._decoder = ParserComposition(broker._decoder, parser.decode_message)
        self.codec = broker.config.broker_codec or DefaultCodec()

    async def publish(self, cmd: "ServiceBusPublishCommand") -> None:
        message = await build_message(
            cmd.body,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            message_id=cmd.message_id,
            reply_to=cmd.reply_to,
            subject=cmd.subject,
            serializer=self.broker.config.fd_config._serializer,
            codec=self.codec,
        )
        await self._route(cmd, message)

    async def publish_batch(self, cmd: "ServiceBusPublishCommand") -> None:
        for body in cmd.batch_bodies:
            message = await build_message(
                body,
                headers=cmd.headers,
                correlation_id=cmd.correlation_id,
                reply_to=cmd.reply_to,
                subject=cmd.subject,
                serializer=self.broker.config.fd_config._serializer,
                codec=self.codec,
            )
            await self._route(cmd, message)

    async def request(self, cmd: "ServiceBusPublishCommand") -> None:
        raise FeatureNotSupportedException(REQUEST_NOT_SUPPORTED)

    async def _route(
        self, cmd: "ServiceBusPublishCommand", message: PatchedMessage
    ) -> None:
        matches = list(
            _matching_subscribers(self.brokers, cmd.destination_type, cmd.destination)
        )
        if cmd.destination_type is DestinationType.Queue:
            matches = matches[:1]
        else:
            matches = list({sub.destination.path: sub for sub in matches}.values())

        for subscriber in matches:
            receiver = PatchedReceiver()
            subscriber._receiver = cast("Any", receiver)
            await subscriber.process_message(cast("Any", message))


def _matching_subscribers(
    brokers: Sequence[ServiceBusBroker],
    destination_type: DestinationType,
    destination: str,
) -> Iterable["ServiceBusSubscriber"]:
    for subscriber in (sub for broker in brokers for sub in broker.subscribers):
        subscriber = cast("ServiceBusSubscriber", subscriber)
        target = subscriber.destination
        if destination_type is DestinationType.Queue:
            if isinstance(target, QueueDestination) and target.name == destination:
                yield subscriber
        elif isinstance(target, SubscriptionDestination) and target.topic == destination:
            yield subscriber


async def build_message(
    message: "SendableMessage",
    *,
    headers: dict[str, Any] | None = None,
    correlation_id: str | None = None,
    message_id: str | None = None,
    reply_to: str = "",
    subject: str | None = None,
    serializer: "SerializerProto | None",
    codec: "CodecProto | None" = None,
) -> PatchedMessage:
    body, content_type = await (codec or DefaultCodec()).encode(message, serializer)
    identifier = message_id or gen_cor_id()
    return PatchedMessage(
        body=(body,),
        application_properties=headers or {},
        content_type=content_type,
        correlation_id=correlation_id or gen_cor_id(),
        message_id=identifier,
        reply_to=reply_to or None,
        subject=subject,
    )
