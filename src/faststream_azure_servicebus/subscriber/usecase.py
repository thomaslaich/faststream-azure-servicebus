import logging
from collections.abc import AsyncIterator, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional

import anyio
from azure.servicebus import ServiceBusReceiveMode, ServiceBusReceivedMessage
from azure.servicebus.aio import AutoLockRenewer
from faststream._internal.endpoint.subscriber import SubscriberUsecase
from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.endpoint.utils import process_msg
from typing_extensions import override

from faststream_azure_servicebus.parser import ServiceBusParser
from faststream_azure_servicebus.publisher.fake import ServiceBusFakePublisher

if TYPE_CHECKING:
    from azure.servicebus.aio import ServiceBusReceiver
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage

    from faststream_azure_servicebus.configs import ServiceBusBrokerConfig
    from faststream_azure_servicebus.message import ServiceBusMessage
    from faststream_azure_servicebus.schemas import Destination
    from faststream_azure_servicebus.subscriber.config import ServiceBusSubscriberConfig


# How long to wait before retrying after a receive fails, so a persistent AMQP
# fault doesn't turn into a busy loop.
CONSUME_ERROR_BACKOFF_SECONDS = 5.0


class ServiceBusSubscriber(TasksMixin, SubscriberUsecase[ServiceBusReceivedMessage]):
    """Receives from one Service Bus queue or topic subscription."""

    _outer_config: "ServiceBusBrokerConfig"

    def __init__(
        self,
        config: "ServiceBusSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
    ) -> None:
        parser = ServiceBusParser(attach=self._attach_receiver)
        config.parser = parser.parse_message
        config.decoder = parser.decode_message

        super().__init__(config, specification, calls)

        self.config = config
        self._receiver: ServiceBusReceiver | None = None
        self._renewer: AutoLockRenewer | None = None
        self._read_lock = anyio.Lock()

    @property
    def destination(self) -> "Destination":
        return self.config.destination.add_prefix(self._outer_config.prefix)

    def _attach_receiver(self, message: "ServiceBusMessage") -> None:
        """Hand a parsed message the receiver that holds its lock."""
        if (
            self._receiver is not None
            and self.config.receive_mode is ServiceBusReceiveMode.PEEK_LOCK
        ):
            message._attach_receiver(self._receiver, self._outer_config)

    def _make_response_publisher(
        self,
        message: "StreamMessage[ServiceBusReceivedMessage]",
    ) -> Sequence["PublisherProto"]:
        return (
            ServiceBusFakePublisher(
                self._outer_config.producer,
                queue=message.reply_to,
            ),
        )

    def get_log_context(
        self,
        message: Optional["StreamMessage[ServiceBusReceivedMessage]"],
    ) -> dict[str, str]:
        return {
            "entity": self.destination.path,
            "message_id": getattr(message, "message_id", ""),
        }

    def _open_receiver(self) -> "ServiceBusReceiver":
        return self.destination.create_receiver(
            self._outer_config.connection.client,
            receive_mode=self.config.receive_mode,
            prefetch_count=self.config.prefetch_count,
        )

    @override
    async def start(self) -> None:
        await super().start()

        self._receiver = self._open_receiver()

        if self.config.should_renew_locks:
            self._renewer = AutoLockRenewer()

        # Give `broker.ping()` an entity it is allowed to peek at.
        self._outer_config.connection.register_probe(
            lambda client: self.destination.create_receiver(client, max_wait_time=2),
        )

        self._post_start()

        start_signal = anyio.Event()

        if self.calls:
            self.add_task(self._consume, func_kwargs={"start_signal": start_signal})

            with anyio.fail_after(3.0):
                await start_signal.wait()
        else:
            start_signal.set()

    @override
    async def stop(self) -> None:
        with anyio.move_on_after(self._outer_config.graceful_timeout):
            async with self._read_lock:
                await super().stop()

        if self._renewer is not None:
            with suppress(Exception):
                await self._renewer.close()
            self._renewer = None

        if self._receiver is not None:
            with suppress(Exception):
                await self._receiver.close()
            self._receiver = None

    async def _consume(self, *, start_signal: anyio.Event) -> None:
        # The receiver was opened in `start()`, so the subscriber is live as soon
        # as this task runs. Signalling before the first fetch keeps startup from
        # blocking for a whole `max_wait_time`.
        start_signal.set()

        connected = True

        while self.running:
            try:
                await self._get_msgs()

            # Any transport failure must be survivable: the loop is the only
            # thing keeping this subscriber alive.
            except Exception as e:  # noqa: BLE001, PERF203
                if connected:
                    self._log(
                        log_level=logging.ERROR,
                        message="Message fetch error",
                        exc_info=e,
                    )
                    connected = False

                await anyio.sleep(CONSUME_ERROR_BACKOFF_SECONDS)

            else:
                connected = True

    async def _get_msgs(self) -> None:
        assert self._receiver is not None

        async with self._read_lock:
            messages = await self._receiver.receive_messages(
                max_message_count=self.config.prefetch_count,
                max_wait_time=self.config.max_wait_time,
            )

        for message in messages:
            self._register_for_renewal(message)
            await self.consume_one(message)

    def _register_for_renewal(self, message: ServiceBusReceivedMessage) -> None:
        """Keep a message's lock alive while a slow handler runs."""
        if self._renewer is None or self._receiver is None:
            return

        self._renewer.register(
            self._receiver,
            message,
            max_lock_renewal_duration=self.config.max_lock_renewal_duration,
        )

    async def consume_one(self, msg: ServiceBusReceivedMessage) -> None:
        await self.consume(msg)

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> Optional["StreamMessage[ServiceBusReceivedMessage]"]:
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )
        assert self._receiver is not None, "Subscriber is not started yet."

        async with self._read_lock:
            received = await self._receiver.receive_messages(
                max_message_count=1,
                max_wait_time=timeout,
            )

        if not received:
            return None

        self._register_for_renewal(received[0])
        return await self._process_one(received[0])

    # The base declares `__aiter__` as a coroutine returning an iterator; every
    # broker implements it as an async generator instead.
    @override
    async def __aiter__(  # ty: ignore[invalid-method-override]
        self,
    ) -> AsyncIterator["StreamMessage[ServiceBusReceivedMessage]"]:
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )
        assert self._receiver is not None, "Subscriber is not started yet."

        while True:
            async with self._read_lock:
                received = await self._receiver.receive_messages(
                    max_message_count=self.config.prefetch_count,
                    max_wait_time=self.config.max_wait_time,
                )

            for message in received:
                self._register_for_renewal(message)
                if (parsed := await self._process_one(message)) is not None:
                    yield parsed

    async def _process_one(
        self,
        message: ServiceBusReceivedMessage,
    ) -> Optional["StreamMessage[ServiceBusReceivedMessage]"]:
        context = self._outer_config.fd_config.context
        parser, decoder = self._get_parser_and_decoder()

        return await process_msg(
            msg=message,
            middlewares=(m(message, context=context) for m in self._broker_middlewares),
            parser=parser,
            decoder=decoder,
        )


class ServiceBusConcurrentSubscriber(
    ConcurrentMixin[ServiceBusReceivedMessage],
    ServiceBusSubscriber,
):
    """Runs up to `max_workers` handlers at once over one receiver."""

    def __init__(
        self,
        config: "ServiceBusSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Any]",
        max_workers: int,
    ) -> None:
        super().__init__(config, specification, calls, max_workers=max_workers)

    @override
    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    @override
    async def consume_one(self, msg: ServiceBusReceivedMessage) -> None:
        await self._put_msg(msg)
