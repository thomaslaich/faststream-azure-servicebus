from typing import Annotated

from azure.servicebus.aio import ServiceBusClient as AzureServiceBusClient
from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.params import NoCast

from faststream_azure_servicebus.broker.broker import ServiceBusBroker as SBBroker
from faststream_azure_servicebus.message import ServiceBusMessage as SBMessage
from faststream_azure_servicebus.publisher.producer import (
    ServiceBusProducer as SBProducer,
)

__all__ = (
    "ContextRepo",
    "Logger",
    "NoCast",
    "ServiceBusBroker",
    "ServiceBusClient",
    "ServiceBusMessage",
    "ServiceBusProducer",
)

ServiceBusMessage = Annotated[SBMessage, Context("message")]
ServiceBusBroker = Annotated[SBBroker, Context("broker")]
ServiceBusClient = Annotated[AzureServiceBusClient, Context("broker._connection")]
ServiceBusProducer = Annotated[SBProducer, Context("broker._producer")]
