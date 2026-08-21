from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.configs import BrokerConfig
from faststream._internal.parser import DefaultCodec
from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from faststream_azure_servicebus.configs.state import ServiceBusConnectionState
    from faststream_azure_servicebus.publisher.producer import ServiceBusProducer


@dataclass(kw_only=True)
class ServiceBusBrokerConfig(BrokerConfig):
    # A connected Service Bus config always has its concrete producer. This
    # intentionally replaces BrokerConfig's ProducerUnset default.
    producer: "ServiceBusProducer"  # pyright: ignore[reportIncompatibleVariableOverride, reportGeneralTypeIssues]
    connection: "ServiceBusConnectionState"

    async def connect(self) -> None:
        self.producer.connect(
            self.fd_config._serializer,
            codec=self.broker_codec or DefaultCodec(),
        )
        await self.connection.connect()

    async def disconnect(self) -> None:
        await self.connection.disconnect()


@dataclass(kw_only=True)
class ServiceBusRouterConfig(BrokerConfig):
    @property
    def connection(self) -> "ServiceBusConnectionState":
        raise IncorrectState
