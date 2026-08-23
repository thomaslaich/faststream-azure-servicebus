from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)

from faststream_azure_servicebus.response import DestinationType

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from faststream_azure_servicebus.configs import ServiceBusBrokerConfig


@dataclass(kw_only=True)
class ServiceBusPublisherSpecificationConfig(PublisherSpecificationConfig):
    destination_type: DestinationType
    destination: str


@dataclass(kw_only=True)
class ServiceBusPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "ServiceBusBrokerConfig"

    destination_type: DestinationType
    destination: str
    headers: dict[str, Any] | None
    reply_to: str
    subject: str | None
    session_id: str | None
    partition_key: str | None
    time_to_live: "timedelta | None"
    scheduled_enqueue_time: "datetime | None"
