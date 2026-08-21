from typing import TYPE_CHECKING, Union

from faststream._internal.endpoint.publisher.fake import FakePublisher

from faststream_azure_servicebus.response import ServiceBusPublishCommand

if TYPE_CHECKING:
    from faststream._internal.producer import ProducerProto
    from faststream.response.response import PublishCommand


class ServiceBusFakePublisher(FakePublisher):
    """Publishes a handler's return value to the message's `reply_to` queue."""

    def __init__(
        self,
        producer: "ProducerProto[ServiceBusPublishCommand]",
        queue: str,
    ) -> None:
        super().__init__(producer=producer)
        self.queue = queue

    def patch_command(
        self,
        cmd: Union["PublishCommand", "ServiceBusPublishCommand"],
    ) -> "ServiceBusPublishCommand":
        cmd = super().patch_command(cmd)
        real_cmd = ServiceBusPublishCommand.from_cmd(cmd)
        real_cmd.set_destination(queue=self.queue)
        return real_cmd
