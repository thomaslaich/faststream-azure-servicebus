from typing import TYPE_CHECKING, Any

from faststream_azure_servicebus.response import resolve_publish_destination

from .config import (
    ServiceBusPublisherConfig,
    ServiceBusPublisherSpecificationConfig,
)
from .specification import ServiceBusPublisherSpecification
from .usecase import ServiceBusPublisher

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from faststream_azure_servicebus.configs import ServiceBusBrokerConfig


def create_publisher(
    *,
    queue: str | None,
    topic: str | None,
    headers: dict[str, Any] | None,
    reply_to: str,
    subject: str | None,
    session_id: str | None,
    partition_key: str | None,
    time_to_live: "timedelta | None",
    scheduled_enqueue_time: "datetime | None",
    config: "ServiceBusBrokerConfig",
    title_: str | None,
    description_: str | None,
    schema_: Any | None,
    include_in_schema: bool,
) -> ServiceBusPublisher:
    destination_type, destination = resolve_publish_destination(
        queue=queue,
        topic=topic,
    )

    publisher_config = ServiceBusPublisherConfig(
        _outer_config=config,
        destination_type=destination_type,
        destination=destination,
        headers=headers,
        reply_to=reply_to,
        subject=subject,
        session_id=session_id,
        partition_key=partition_key,
        time_to_live=time_to_live,
        scheduled_enqueue_time=scheduled_enqueue_time,
    )
    specification = ServiceBusPublisherSpecification(
        config,
        ServiceBusPublisherSpecificationConfig(
            destination_type=destination_type,
            destination=destination,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )
    return ServiceBusPublisher(publisher_config, specification)
