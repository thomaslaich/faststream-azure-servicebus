from faststream._internal.parser import ParserProto
from faststream._internal.testing.app import TestApp

ServiceBusParserType = ParserProto["ServiceBusReceivedMessage"]  # type: ignore[name-defined]  # ty: ignore[unresolved-reference]

try:
    from .annotations import ServiceBusMessage
    from .broker import (
        ServiceBusBroker,
        ServiceBusPublisherArgs,
        ServiceBusRoute,
        ServiceBusRouter,
    )
    from .publisher import ServiceBusPublisher
    from .response import ServiceBusPublishCommand, ServiceBusResponse
    from .testing import TestServiceBusBroker

except ImportError as e:
    if "'azure" not in e.msg:
        raise

    from .exceptions import INSTALL_AZURE_SERVICEBUS

    raise ImportError(INSTALL_AZURE_SERVICEBUS) from e

__all__ = (
    "ServiceBusBroker",
    "ServiceBusMessage",
    "ServiceBusParserType",
    "ServiceBusPublishCommand",
    "ServiceBusPublisher",
    "ServiceBusPublisherArgs",
    "ServiceBusResponse",
    "ServiceBusRoute",
    "ServiceBusRouter",
    "TestApp",
    "TestServiceBusBroker",
)
