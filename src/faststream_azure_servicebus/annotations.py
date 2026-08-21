from typing import Annotated

from faststream.params import Context

from faststream_azure_servicebus.broker.broker import ServiceBusBroker as SBBroker
from faststream_azure_servicebus.message import ServiceBusMessage as SBMessage

__all__ = ("ServiceBusBroker", "ServiceBusMessage")

ServiceBusMessage = Annotated[SBMessage, Context("message")]
ServiceBusBroker = Annotated[SBBroker, Context("broker")]
