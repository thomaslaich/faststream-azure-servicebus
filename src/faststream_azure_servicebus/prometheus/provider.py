from typing import TYPE_CHECKING, cast

from faststream.prometheus import MetricsSettingsProvider

if TYPE_CHECKING:
    from azure.servicebus import ServiceBusReceivedMessage
    from faststream.message import StreamMessage
    from faststream.prometheus import ConsumeAttrs

    from faststream_azure_servicebus.message import ServiceBusMessage
    from faststream_azure_servicebus.response import ServiceBusPublishCommand


MISSING_DESTINATION = "Consumed Service Bus message has no source destination."


class ServiceBusMetricsSettingsProvider(
    MetricsSettingsProvider["ServiceBusReceivedMessage", "ServiceBusPublishCommand"],
):
    messaging_system = "servicebus"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[ServiceBusReceivedMessage]",
    ) -> "ConsumeAttrs":
        destination = cast("ServiceBusMessage", msg).destination
        if destination is None:
            raise RuntimeError(MISSING_DESTINATION)
        return {
            "destination_name": destination.path,
            "message_size": len(msg.body),
            "messages_count": 1,
        }

    def get_publish_destination_name_from_cmd(
        self,
        cmd: "ServiceBusPublishCommand",
    ) -> str:
        return cmd.destination


def settings_provider_factory(
    msg: "ServiceBusReceivedMessage | None",
) -> ServiceBusMetricsSettingsProvider:
    return ServiceBusMetricsSettingsProvider()
