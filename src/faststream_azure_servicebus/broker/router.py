from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any

from azure.servicebus import ServiceBusReceivedMessage
from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream._internal.constants import EMPTY

from faststream_azure_servicebus.configs import ServiceBusRouterConfig

from .registrator import ServiceBusRegistrator

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from fast_depends.dependencies import Dependant
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.middlewares import AckPolicy


class ServiceBusPublisherArgs(ArgsContainer):
    """Arguments for a publisher declared as part of a delayed route."""

    def __init__(
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
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            queue=queue,
            topic=topic,
            headers=headers,
            reply_to=reply_to,
            subject=subject,
            session_id=session_id,
            partition_key=partition_key,
            time_to_live=time_to_live,
            scheduled_enqueue_time=scheduled_enqueue_time,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class ServiceBusRoute(SubscriberRoute):
    """A subscriber and its publishers, registered when a router is built."""

    def __init__(
        self,
        call: Callable[..., "SendableMessage"]
        | Callable[..., Awaitable["SendableMessage"]],
        queue: str | None = None,
        *,
        topic: str | None = None,
        subscription: str | None = None,
        publishers: Iterable[ServiceBusPublisherArgs] = (),
        prefetch_count: int = 1,
        max_wait_time: float | None = 5.0,
        max_lock_renewal_duration: float | None = 300.0,
        max_workers: int = 1,
        ack_policy: "AckPolicy" = EMPTY,
        no_reply: bool = False,
        dependencies: Iterable["Dependant"] = (),
        parser: "CustomCallable | None" = None,
        decoder: "CustomCallable | None" = None,
        codec: "CodecProto | None" = None,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            call,
            publishers=publishers,
            queue=queue,
            topic=topic,
            subscription=subscription,
            prefetch_count=prefetch_count,
            max_wait_time=max_wait_time,
            max_lock_renewal_duration=max_lock_renewal_duration,
            max_workers=max_workers,
            ack_policy=ack_policy,
            no_reply=no_reply,
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            codec=codec,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
        )


class ServiceBusRouter(
    ServiceBusRegistrator[ServiceBusRouterConfig],
    BrokerRouter[ServiceBusReceivedMessage, ServiceBusRouterConfig],
):
    """An includable collection of Service Bus subscribers and publishers."""

    def __init__(
        self,
        prefix: str = "",
        handlers: Iterable[ServiceBusRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[ServiceBusRegistrator[Any]] = (),
        parser: "CustomCallable | None" = None,
        decoder: "CustomCallable | None" = None,
        codec: "CodecProto | None" = None,
        include_in_schema: bool | None = None,
        ack_policy: "AckPolicy" = EMPTY,
    ) -> None:
        BrokerRouter.__init__(
            self,
            handlers=handlers,
            routers=routers,
            config=ServiceBusRouterConfig(
                prefix=prefix,
                broker_dependencies=dependencies,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                broker_codec=codec,
                include_in_schema=include_in_schema,
                ack_policy=ack_policy,
            ),
        )
