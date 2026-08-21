from typing import TYPE_CHECKING, Any

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy

from faststream_azure_servicebus.schemas import (
    Destination,
    QueueDestination,
    SubscriptionDestination,
)

from .config import (
    ServiceBusSubscriberConfig,
    ServiceBusSubscriberSpecificationConfig,
)
from .specification import ServiceBusSubscriberSpecification
from .usecase import ServiceBusConcurrentSubscriber, ServiceBusSubscriber

if TYPE_CHECKING:
    from faststream_azure_servicebus.configs import ServiceBusBrokerConfig

INCORRECT_SETUP_MSG = (
    "You have to specify either `queue`, or both `topic` and `subscription`."
)


def resolve_destination(
    queue: str | None,
    topic: str | None,
    subscription: str | None,
) -> Destination:
    if queue is not None:
        if topic is not None or subscription is not None:
            raise SetupError(INCORRECT_SETUP_MSG)
        return QueueDestination(name=queue)

    if topic is not None:
        if subscription is None:
            msg = (
                "A topic subscriber needs a `subscription`: a topic can only be "
                "consumed through one."
            )
            raise SetupError(msg)
        return SubscriptionDestination(topic=topic, subscription=subscription)

    raise SetupError(INCORRECT_SETUP_MSG)


def create_subscriber(
    *,
    queue: str | None,
    topic: str | None,
    subscription: str | None,
    config: "ServiceBusBrokerConfig",
    ack_policy: "AckPolicy" = EMPTY,
    no_reply: bool = False,
    prefetch_count: int = 1,
    max_wait_time: float | None = 5.0,
    max_lock_renewal_duration: float | None = 300.0,
    max_workers: int = 1,
    # AsyncAPI args
    title_: str | None = None,
    description_: str | None = None,
    include_in_schema: bool = True,
) -> ServiceBusSubscriber:
    destination = resolve_destination(queue, topic, subscription)

    _validate_input_for_misconfigure(
        ack_policy=ack_policy,
        prefetch_count=prefetch_count,
        max_workers=max_workers,
    )

    subscriber_config = ServiceBusSubscriberConfig(
        destination=destination,
        no_reply=no_reply,
        _outer_config=config,
        _ack_policy=ack_policy,
        prefetch_count=prefetch_count,
        max_wait_time=max_wait_time,
        max_lock_renewal_duration=max_lock_renewal_duration,
    )

    specification_config = ServiceBusSubscriberSpecificationConfig(
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    calls = CallsCollection[Any]()

    specification = ServiceBusSubscriberSpecification(
        config,
        specification_config,
        calls,
        destination=destination,
    )

    if max_workers > 1:
        return ServiceBusConcurrentSubscriber(
            subscriber_config,
            specification,
            calls,
            max_workers=max_workers,
        )

    return ServiceBusSubscriber(subscriber_config, specification, calls)


def _validate_input_for_misconfigure(
    *,
    ack_policy: "AckPolicy",
    prefetch_count: int,
    max_workers: int,
) -> None:
    if prefetch_count < 1:
        msg = "`prefetch_count` must be at least 1."
        raise SetupError(msg)

    if max_workers < 1:
        msg = "`max_workers` must be at least 1."
        raise SetupError(msg)

    if max_workers > 1 and prefetch_count < max_workers:
        msg = (
            f"`prefetch_count={prefetch_count}` starves `max_workers={max_workers}`: "
            "fetch at least as many messages as there are workers."
        )
        raise SetupError(msg)

    if ack_policy is AckPolicy.ACK_FIRST and max_workers > 1:
        # RECEIVE_AND_DELETE drops anything in flight when a worker dies, and
        # concurrency widens that window considerably.
        msg = (
            "`AckPolicy.ACK_FIRST` with `max_workers > 1` loses messages on "
            "handler failure. Use `AckPolicy.ACK` instead."
        )
        raise SetupError(msg)
