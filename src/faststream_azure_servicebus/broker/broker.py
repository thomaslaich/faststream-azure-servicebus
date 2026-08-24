import logging
import re
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

import anyio
from azure.servicebus import ServiceBusReceivedMessage
from fast_depends import Provider, dependency_provider
from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.message import gen_cor_id
from faststream.response.publish_type import PublishType
from faststream.specification.schema.broker import BrokerSpec
from typing_extensions import override

from faststream_azure_servicebus.broker.logging import make_servicebus_logger_state
from faststream_azure_servicebus.broker.registrator import ServiceBusRegistrator
from faststream_azure_servicebus.configs import (
    ServiceBusBrokerConfig,
    ServiceBusConnectionState,
    build_client_factory,
)
from faststream_azure_servicebus.publisher.producer import ServiceBusProducer
from faststream_azure_servicebus.response import ServiceBusPublishCommand

if TYPE_CHECKING:
    from datetime import datetime, timedelta
    from types import TracebackType

    from azure.servicebus.aio import ServiceBusClient
    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.basic_types import LoggerProto, SendableMessage
    from faststream._internal.broker.registrator import Registrator
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.middlewares import AckPolicy
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra.tag import Tag, TagDict

    from faststream_azure_servicebus.message import ServiceBusMessage

# `Endpoint=sb://<host>/;SharedAccessKeyName=...` — the only part of a connection
# string worth putting in the AsyncAPI document (it carries no secret).
_ENDPOINT_PATTERN = re.compile(r"Endpoint=(?P<endpoint>[^;]+)", re.IGNORECASE)


def _specification_url(
    connection_string: str | None,
    fully_qualified_namespace: str | None,
) -> str:
    if fully_qualified_namespace:
        return f"amqps://{fully_qualified_namespace}"

    if connection_string and (match := _ENDPOINT_PATTERN.search(connection_string)):
        return match.group("endpoint").rstrip("/")

    return "amqps://localhost"


class ServiceBusBroker(  # pyright: ignore[reportIncompatibleMethodOverride]
    ServiceBusRegistrator,
    BrokerUsecase[
        ServiceBusReceivedMessage,
        "ServiceBusClient",
        ServiceBusBrokerConfig,
    ],
):
    """Azure Service Bus broker."""

    def __init__(
        self,
        connection_string: str | None = None,
        *,
        fully_qualified_namespace: str | None = None,
        credential: Any = None,
        reply_queue: str | None = None,
        # client options
        retry_total: int = 3,
        retry_backoff_factor: float = 0.8,
        retry_backoff_max: float = 120.0,
        transport_type: Any = None,
        custom_endpoint_address: str | None = None,
        connection_verify: str | None = None,
        logging_enable: bool = False,
        client_options: dict[str, Any] | None = None,
        # broker base args
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any]"] = (),
        routers: Iterable["Registrator[Any, Any]"] = (),
        ack_policy: "AckPolicy" = EMPTY,
        # AsyncAPI args
        security: Optional["BaseSecurity"] = None,
        specification_url: str | None = None,
        protocol: str | None = "amqp",
        protocol_version: str | None = "1.0",
        description: str | None = None,
        tags: Iterable[Union["Tag", "TagDict"]] = (),
        # logging args
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        # FastDepends args
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
    ) -> None:
        """Args:
        connection_string: A Service Bus connection string. Mutually exclusive
            with `fully_qualified_namespace` / `credential`.
        fully_qualified_namespace: `<namespace>.servicebus.windows.net`.
        credential: A `TokenCredential` from `azure-identity`, e.g.
            `DefaultAzureCredential()`. Requires `fully_qualified_namespace`.
        reply_queue: Existing queue used exclusively by this broker instance to
            receive replies for `request()` calls.
        retry_total: How many times the SDK retries a failed operation.
        retry_backoff_factor: Base of the SDK's exponential retry backoff.
        retry_backoff_max: Ceiling on the SDK's retry backoff, in seconds.
        transport_type: `azure.servicebus.TransportType`. Defaults to AMQP over
            TCP; the emulator supports nothing else.
        custom_endpoint_address: Endpoint to route the connection through.
        connection_verify: Path to a custom CA bundle.
        logging_enable: Log AMQP frames. Very noisy; debugging only.
        client_options: Anything else to pass through to `ServiceBusClient`.
        graceful_timeout: Seconds to let handlers finish during shutdown.
        decoder: Custom decoder for all subscribers.
        parser: Custom parser for all subscribers.
        codec: Custom codec for all endpoints.
        dependencies: Dependencies applied to all subscribers.
        middlewares: Broker middlewares.
        routers: Routers to include at construction time.
        ack_policy: Default settlement policy for all subscribers.
        security: Security scheme for the AsyncAPI document.
        specification_url: Overrides the URL shown in the AsyncAPI document.
        protocol: Protocol name for the AsyncAPI document.
        protocol_version: Protocol version for the AsyncAPI document.
        description: Broker description for the AsyncAPI document.
        tags: Tags for the AsyncAPI document.
        logger: Custom logger; `None` disables logging.
        log_level: Level for the broker's own log records.
        apply_types: Whether to use FastDepends type casting.
        serializer: Custom serializer for message bodies.
        provider: FastDepends dependency provider.
        context: FastStream context repository.
        """
        options: dict[str, Any] = {
            "retry_total": retry_total,
            "retry_backoff_factor": retry_backoff_factor,
            "retry_backoff_max": retry_backoff_max,
            "logging_enable": logging_enable,
            **(client_options or {}),
        }
        if transport_type is not None:
            options["transport_type"] = transport_type
        if custom_endpoint_address is not None:
            options["custom_endpoint_address"] = custom_endpoint_address
        if connection_verify is not None:
            options["connection_verify"] = connection_verify

        connection = ServiceBusConnectionState(
            build_client_factory(
                connection_string=connection_string,
                fully_qualified_namespace=fully_qualified_namespace,
                credential=credential,
                options=options,
            ),
        )
        self.reply_queue = reply_queue

        producer = ServiceBusProducer(
            connection=connection,
            parser=parser,
            decoder=decoder,
            reply_queue=reply_queue,
            serializer=None if serializer is EMPTY else serializer,
            codec=codec,
        )

        super().__init__(
            routers=routers,
            config=ServiceBusBrokerConfig(
                connection=connection,
                producer=producer,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                broker_codec=codec,
                logger=make_servicebus_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
                ack_policy=ack_policy,
                extra_context={"broker": self},
            ),
            specification=BrokerSpec(
                description=description,
                url=[
                    specification_url
                    or _specification_url(
                        connection_string,
                        fully_qualified_namespace,
                    ),
                ],
                protocol=protocol,
                protocol_version=protocol_version,
                security=security,
                tags=tags,
            ),
        )

    @override
    async def _connect(self) -> "ServiceBusClient":
        await self.config.connect()
        return self.config.broker_config.connection.client

    async def start(self) -> None:
        await self.connect()
        await super().start()

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)
        await self.config.disconnect()
        self._connection = None

    @override
    async def publish(  # type: ignore[override]
        self,
        message: "SendableMessage" = None,
        queue: str | None = None,
        *,
        topic: str | None = None,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        reply_to: str = "",
        subject: str | None = None,
        session_id: str | None = None,
        partition_key: str | None = None,
        time_to_live: Optional["timedelta"] = None,
        scheduled_enqueue_time: Optional["datetime"] = None,
    ) -> None:
        """Publish a message to a queue or a topic.

        Args:
            message: The message body.
            queue: Queue to publish to. Mutually exclusive with `topic`.
            topic: Topic to publish to. Mutually exclusive with `queue`.
            headers: Application properties to attach.
            correlation_id: Correlation id, useful for tracing.
            message_id: Message id. Also the key for duplicate detection.
            reply_to: Entity a consumer should reply to.
            subject: The message's subject (its "label").
            session_id: Session id, for session-enabled entities.
            partition_key: Partition key, for partitioned entities.
            time_to_live: How long the message stays alive in the entity.
            scheduled_enqueue_time: Hold the message until this UTC time.
        """
        cmd = ServiceBusPublishCommand(
            message,
            queue=queue,
            topic=topic,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            message_id=message_id,
            reply_to=reply_to,
            subject=subject,
            session_id=session_id,
            partition_key=partition_key,
            time_to_live=time_to_live,
            scheduled_enqueue_time=scheduled_enqueue_time,
            _publish_type=PublishType.PUBLISH,
        )

        await super()._basic_publish(cmd, producer=self.config.producer)

    @override
    async def publish_batch(  # type: ignore[override]
        self,
        *messages: "SendableMessage",
        queue: str | None = None,
        topic: str | None = None,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        subject: str | None = None,
        session_id: str | None = None,
        partition_key: str | None = None,
        time_to_live: Optional["timedelta"] = None,
        scheduled_enqueue_time: Optional["datetime"] = None,
    ) -> None:
        """Publish several messages in as few service calls as possible.

        Messages are packed into batches of at most 256 KB; oversized batches
        roll over into another send.

        Args:
            *messages: The message bodies.
            queue: Queue to publish to. Mutually exclusive with `topic`.
            topic: Topic to publish to. Mutually exclusive with `queue`.
            headers: Application properties to attach to every message.
            correlation_id: Correlation id, useful for tracing.
            reply_to: Entity a consumer should reply to.
            subject: The messages' subject (their "label").
            session_id: Session id, for session-enabled entities.
            partition_key: Partition key, for partitioned entities.
            time_to_live: How long the messages stay alive in the entity.
            scheduled_enqueue_time: Hold the messages until this UTC time.
        """
        if not messages:
            return

        cmd = ServiceBusPublishCommand(
            *messages,
            queue=queue,
            topic=topic,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            reply_to=reply_to,
            subject=subject,
            session_id=session_id,
            partition_key=partition_key,
            time_to_live=time_to_live,
            scheduled_enqueue_time=scheduled_enqueue_time,
            _publish_type=PublishType.PUBLISH,
        )

        await super()._basic_publish_batch(cmd, producer=self.config.producer)

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage" = None,
        queue: str | None = None,
        *,
        topic: str | None = None,
        timeout: float | None = 30.0,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        subject: str | None = None,
        session_id: str | None = None,
        partition_key: str | None = None,
        time_to_live: Optional["timedelta"] = None,
        scheduled_enqueue_time: Optional["datetime"] = None,
    ) -> "ServiceBusMessage":  # ty: ignore[invalid-method-override]
        """Publish a request and wait for its correlated reply.

        The broker must be constructed with an existing `reply_queue`. One
        shared receiver serves every concurrent request made by this broker.
        """
        identifier = message_id or gen_cor_id()
        cmd = ServiceBusPublishCommand(
            message,
            queue=queue,
            topic=topic,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            message_id=identifier,
            reply_to=self.reply_queue or "",
            subject=subject,
            session_id=session_id,
            partition_key=partition_key,
            time_to_live=time_to_live,
            scheduled_enqueue_time=scheduled_enqueue_time,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
        )

        response: ServiceBusMessage = await super()._basic_request(
            cmd,
            producer=self.config.producer,
        )
        return response

    @override
    async def ping(self, timeout: float | None = 3) -> bool:
        with anyio.move_on_after(timeout) as cancel_scope:
            if self._connection is None:
                return False

            while True:
                if cancel_scope.cancel_called:
                    return False

                if await self.config.broker_config.connection.ping():
                    return True

                await anyio.sleep((timeout or 10) / 10)

        return False
