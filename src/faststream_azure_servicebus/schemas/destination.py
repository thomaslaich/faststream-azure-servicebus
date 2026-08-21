from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from azure.servicebus.aio import ServiceBusClient, ServiceBusReceiver


class Destination(Protocol):
    """Somewhere a subscriber can receive from."""

    @property
    def path(self) -> str:
        """The entity path, as it appears in logs and the AsyncAPI document."""
        ...

    def add_prefix(self, prefix: str) -> "Destination":
        """Return a copy with a router prefix applied."""
        ...

    def create_receiver(
        self,
        client: "ServiceBusClient",
        **kwargs: Any,
    ) -> "ServiceBusReceiver":
        """Open a receiver for this entity."""
        ...


@dataclass(frozen=True)
class QueueDestination:
    name: str

    @property
    def path(self) -> str:
        return self.name

    def add_prefix(self, prefix: str) -> "QueueDestination":
        return replace(self, name=f"{prefix}{self.name}")

    def create_receiver(
        self,
        client: "ServiceBusClient",
        **kwargs: Any,
    ) -> "ServiceBusReceiver":
        return client.get_queue_receiver(queue_name=self.name, **kwargs)


@dataclass(frozen=True)
class SubscriptionDestination:
    topic: str
    subscription: str

    @property
    def path(self) -> str:
        return f"{self.topic}/{self.subscription}"

    def add_prefix(self, prefix: str) -> "SubscriptionDestination":
        return replace(self, topic=f"{prefix}{self.topic}")

    def create_receiver(
        self,
        client: "ServiceBusClient",
        **kwargs: Any,
    ) -> "ServiceBusReceiver":
        return client.get_subscription_receiver(
            topic_name=self.topic,
            subscription_name=self.subscription,
            **kwargs,
        )
