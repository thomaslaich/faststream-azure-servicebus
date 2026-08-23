from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Generic, cast

from azure.servicebus import ServiceBusReceivedMessage
from faststream._internal.broker.registrator import Registrator
from faststream._internal.configs import BrokerConfig
from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from typing_extensions import TypeVar, override

from faststream_azure_servicebus.configs import ServiceBusBrokerConfig
from faststream_azure_servicebus.publisher.factory import create_publisher
from faststream_azure_servicebus.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from fast_depends.dependencies import Dependant
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.middlewares import AckPolicy

    from faststream_azure_servicebus.publisher import ServiceBusPublisher
    from faststream_azure_servicebus.subscriber.usecase import ServiceBusSubscriber

ServiceBusConfig = TypeVar(
    "ServiceBusConfig",
    bound=BrokerConfig,
    default=ServiceBusBrokerConfig,
)


class ServiceBusRegistrator(
    Registrator[ServiceBusReceivedMessage, ServiceBusConfig],
    Generic[ServiceBusConfig],
):
    """Includable to ServiceBusBroker router."""

    # Deliberately replaces the base's low-level `(subscriber, persistent)` hook
    # with the decorator API users actually call, as every broker does.
    def subscriber(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        queue: str | None = None,
        *,
        topic: str | None = None,
        subscription: str | None = None,
        # subscriber args
        prefetch_count: int = 1,
        max_wait_time: float | None = 5.0,
        max_lock_renewal_duration: float | None = 300.0,
        max_workers: int = 1,
        ack_policy: "AckPolicy" = EMPTY,
        no_reply: bool = False,
        # broker args
        dependencies: Iterable["Dependant"] = (),
        parser: "CustomCallable | None" = None,
        decoder: "CustomCallable | None" = None,
        codec: "CodecProto | None" = None,
        persistent: bool = True,
        # AsyncAPI args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "ServiceBusSubscriber":  # ty: ignore[invalid-method-override]
        """Consume from a Service Bus queue, or from a topic subscription.

        Args:
            queue: Queue to consume. Mutually exclusive with `topic`.
            topic: Topic to consume, through `subscription`.
            subscription: Subscription on `topic` to consume. Required with `topic`.
            prefetch_count: How many messages to fetch per service call. Raising it
                improves throughput and costs nothing until the locks start expiring.
            max_wait_time: Seconds a fetch waits for messages before looping.
            max_lock_renewal_duration: How long to keep renewing a message's lock
                while the handler runs. `None` disables renewal, after which a
                handler slower than the entity's lock duration will see the
                message redelivered.
            max_workers: Handlers to run concurrently over this subscriber.
            ack_policy: When to settle. Defaults to `REJECT_ON_ERROR`, which
                dead-letters what the handler could not process.
            no_reply: Suppress automatic replies to `reply_to`.
            dependencies: Dependencies for this subscriber's handlers.
            parser: Custom parser for this subscriber.
            decoder: Custom decoder for this subscriber.
            codec: Custom codec for this subscriber.
            persistent: Keep a strong reference to this subscriber.
            title: Channel name in the AsyncAPI document.
            description: Description in the AsyncAPI document. Defaults to the
                handler's docstring.
            include_in_schema: Whether to show this subscriber in the AsyncAPI
                document.
        """
        subscriber = create_subscriber(
            queue=queue,
            topic=topic,
            subscription=subscription,
            config=cast("ServiceBusBrokerConfig", self.config),
            ack_policy=ack_policy,
            no_reply=no_reply,
            prefetch_count=prefetch_count,
            max_wait_time=max_wait_time,
            max_lock_renewal_duration=max_lock_renewal_duration,
            max_workers=max_workers,
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        )

        super().subscriber(subscriber, persistent=persistent)

        return subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
            dependencies_=dependencies,
            codec_=codec,
        )

    def publisher(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        queue: str | None = None,
        *,
        topic: str | None = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        subject: str | None = None,
        session_id: str | None = None,
        partition_key: str | None = None,
        time_to_live: "timedelta | None" = None,
        scheduled_enqueue_time: "datetime | None" = None,
        # broker args
        persistent: bool = True,
        # AsyncAPI args
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "ServiceBusPublisher":  # ty: ignore[invalid-method-override]
        """Create a reusable queue or topic publisher.

        The returned object can publish directly or decorate a subscriber
        handler, in which case the handler's return value is published.
        """
        publisher = create_publisher(
            queue=queue,
            topic=topic,
            headers=headers,
            reply_to=reply_to,
            subject=subject,
            session_id=session_id,
            partition_key=partition_key,
            time_to_live=time_to_live,
            scheduled_enqueue_time=scheduled_enqueue_time,
            config=cast("ServiceBusBrokerConfig", self.config),
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=include_in_schema,
        )
        super().publisher(publisher, persistent=persistent)
        return publisher

    @override
    def include_router(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        router: "ServiceBusRegistrator[Any]",
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        include_in_schema: bool | None = None,
    ) -> None:  # ty: ignore[invalid-method-override]
        if not isinstance(router, ServiceBusRegistrator):
            msg = (
                "Router must be an instance of ServiceBusRegistrator, "
                f"got {type(router).__name__} instead"
            )
            raise SetupError(msg)

        super().include_router(
            router,
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            include_in_schema=include_in_schema,
        )
