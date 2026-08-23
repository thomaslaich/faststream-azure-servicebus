from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec

from faststream_azure_servicebus.configs import ServiceBusBrokerConfig

from .config import ServiceBusPublisherSpecificationConfig


class ServiceBusPublisherSpecification(
    PublisherSpecification[
        ServiceBusBrokerConfig,
        ServiceBusPublisherSpecificationConfig,
    ],
):
    @property
    def entity_path(self) -> str:
        return f"{self._outer_config.prefix}{self.config.destination}"

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self.entity_path}:Publisher"

    def get_schema(self) -> dict[str, PublisherSpec]:
        return {
            self.name: PublisherSpec(
                description=self.config.description_,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(self.get_payloads(), "Publisher"),
                    ),
                    bindings=None,
                ),
                # AsyncAPI has no Azure Service Bus binding. As with subscribers,
                # the prefixed entity path is represented by the channel name.
                bindings=None,
            ),
        }
