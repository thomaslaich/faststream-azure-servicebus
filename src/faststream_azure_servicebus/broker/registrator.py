from collections.abc import Iterable
from typing import TYPE_CHECKING

from azure.servicebus import ServiceBusReceivedMessage
from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from typing_extensions import override

from faststream_azure_servicebus.configs import ServiceBusBrokerConfig
from faststream_azure_servicebus.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from faststream._internal.endpoint.publisher import PublisherUsecase
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import CustomCallable
    from faststream.middlewares import AckPolicy

    from faststream_azure_servicebus.subscriber.usecase import ServiceBusSubscriber


class ServiceBusRegistrator(
    Registrator[ServiceBusReceivedMessage, ServiceBusBrokerConfig],
):
    """Includable to ServiceBusBroker router."""

    # Deliberately replaces the base's low-level `(subscriber, persistent)` hook
    # with the decorator API users actually call, as every broker does.
    def subscriber(  # ty: ignore[invalid-method-override]
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
        # AsyncAPI args
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "ServiceBusSubscriber":
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
            config=self.config.broker_config,
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

        super().subscriber(subscriber)

        return subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
            dependencies_=dependencies,
            codec_=codec,
        )

    @override
    def publisher(
        self,
        publisher: "PublisherUsecase",
        persistent: bool = True,
    ) -> "PublisherUsecase":
        return super().publisher(publisher, persistent)
