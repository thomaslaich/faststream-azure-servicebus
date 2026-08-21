from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec

from faststream_azure_servicebus.configs import ServiceBusBrokerConfig

from .config import ServiceBusSubscriberSpecificationConfig

if TYPE_CHECKING:
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection

    from faststream_azure_servicebus.schemas import Destination


class ServiceBusSubscriberSpecification(
    SubscriberSpecification[
        ServiceBusBrokerConfig,
        ServiceBusSubscriberSpecificationConfig,
    ],
):
    def __init__(
        self,
        _outer_config: "ServiceBusBrokerConfig",
        specification_config: "ServiceBusSubscriberSpecificationConfig",
        calls: "CallsCollection[Any]",
        destination: "Destination",
    ) -> None:
        super().__init__(_outer_config, specification_config, calls)
        self.destination = destination

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.entity_path}:{self.call_name}"

    @property
    def entity_path(self) -> str:
        return f"{self._outer_config.prefix}{self.destination.path}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        return {
            self.name: SubscriberSpec(
                description=self.description,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                    bindings=None,
                ),
                # AsyncAPI has no Service Bus binding, and inventing one would
                # produce a document no tool understands. The entity path is
                # carried by the channel name instead.
                bindings=None,
            ),
        }
