from typing import TYPE_CHECKING, Any, cast

from faststream.opentelemetry import TelemetrySettingsProvider
from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME

from faststream_azure_servicebus.schemas import Destination, SubscriptionDestination

if TYPE_CHECKING:
    from azure.servicebus import ServiceBusReceivedMessage
    from faststream.message import StreamMessage

    from faststream_azure_servicebus.message import ServiceBusMessage
    from faststream_azure_servicebus.response import ServiceBusPublishCommand


MESSAGING_SYSTEM = "messaging.system"
MESSAGING_DESTINATION_NAME = "messaging.destination.name"
MESSAGING_DESTINATION_SUBSCRIPTION_NAME = "messaging.destination.subscription.name"
MESSAGING_MESSAGE_ID = "messaging.message.id"
MESSAGING_MESSAGE_CONVERSATION_ID = "messaging.message.conversation_id"
MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES = "messaging.message.payload_size_bytes"
MESSAGING_SERVICEBUS_DELIVERY_COUNT = "messaging.servicebus.message.delivery_count"
MESSAGING_SERVICEBUS_ENQUEUED_TIME = "messaging.servicebus.message.enqueued_time"
MISSING_DESTINATION = "Consumed Service Bus message has no source destination."


class ServiceBusTelemetrySettingsProvider(
    TelemetrySettingsProvider["ServiceBusReceivedMessage", "ServiceBusPublishCommand"],
):
    messaging_system = "servicebus"

    def get_publish_attrs_from_cmd(
        self,
        cmd: "ServiceBusPublishCommand",
    ) -> dict[str, Any]:
        attrs: dict[str, Any] = {
            MESSAGING_SYSTEM: self.messaging_system,
            MESSAGING_DESTINATION_NAME: cmd.destination,
        }
        if cmd.message_id is not None:
            attrs[MESSAGING_MESSAGE_ID] = cmd.message_id
        if cmd.correlation_id is not None:
            attrs[MESSAGING_MESSAGE_CONVERSATION_ID] = cmd.correlation_id
        return attrs

    def get_publish_destination_name(self, cmd: "ServiceBusPublishCommand") -> str:
        return cmd.destination

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[ServiceBusReceivedMessage]",
    ) -> dict[str, Any]:
        servicebus_msg = cast("ServiceBusMessage", msg)
        destination = _get_destination(servicebus_msg)

        attrs: dict[str, Any] = {
            MESSAGING_SYSTEM: self.messaging_system,
            MESSAGING_MESSAGE_ID: msg.message_id,
            MESSAGING_MESSAGE_CONVERSATION_ID: msg.correlation_id,
            MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.body),
            MESSAGING_DESTINATION_PUBLISH_NAME: destination.path,
        }
        if isinstance(destination, SubscriptionDestination):
            attrs[MESSAGING_DESTINATION_SUBSCRIPTION_NAME] = destination.subscription
        if servicebus_msg.delivery_count is not None:
            attrs[MESSAGING_SERVICEBUS_DELIVERY_COUNT] = servicebus_msg.delivery_count
        if enqueued_time := getattr(msg.raw_message, "enqueued_time_utc", None):
            attrs[MESSAGING_SERVICEBUS_ENQUEUED_TIME] = int(enqueued_time.timestamp())
        return attrs

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[ServiceBusReceivedMessage]",
    ) -> str:
        return _get_destination(cast("ServiceBusMessage", msg)).path


def _get_destination(msg: "ServiceBusMessage") -> Destination:
    if msg.destination is None:
        raise RuntimeError(MISSING_DESTINATION)
    return msg.destination


def telemetry_attributes_provider_factory(
    msg: "ServiceBusReceivedMessage | None",
) -> ServiceBusTelemetrySettingsProvider:
    return ServiceBusTelemetrySettingsProvider()
