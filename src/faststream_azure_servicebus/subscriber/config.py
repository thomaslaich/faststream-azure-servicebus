from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from azure.servicebus import ServiceBusReceiveMode
from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares.acknowledgement.config import AckPolicy

from faststream_azure_servicebus.configs import ServiceBusBrokerConfig

if TYPE_CHECKING:
    from faststream_azure_servicebus.schemas import Destination


class ServiceBusSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    pass


@dataclass(kw_only=True)
class ServiceBusSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: ServiceBusBrokerConfig

    destination: "Destination" = field(repr=False)

    prefetch_count: int = 1
    max_wait_time: float | None = 5.0
    max_lock_renewal_duration: float | None = 300.0

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is not EMPTY:
            return self._ack_policy

        if self._outer_config.ack_policy is not EMPTY:
            return self._outer_config.ack_policy

        # Service Bus redelivers anything left unsettled, so the safe default is
        # to dead-letter what the handler could not process rather than let it
        # loop until `MaxDeliveryCount`.
        return AckPolicy.REJECT_ON_ERROR

    @property
    def receive_mode(self) -> ServiceBusReceiveMode:
        """Take the lock, unless the policy says the message is done on arrival.

        `ACK_FIRST` means "settle before the handler runs", which Service Bus
        expresses natively as `RECEIVE_AND_DELETE` — one round trip instead of
        two, at the cost of losing the message if the handler dies.
        """
        if self.ack_policy is AckPolicy.ACK_FIRST:
            return ServiceBusReceiveMode.RECEIVE_AND_DELETE

        return ServiceBusReceiveMode.PEEK_LOCK

    @property
    def should_renew_locks(self) -> bool:
        return (
            self.max_lock_renewal_duration is not None
            and self.receive_mode is ServiceBusReceiveMode.PEEK_LOCK
        )
