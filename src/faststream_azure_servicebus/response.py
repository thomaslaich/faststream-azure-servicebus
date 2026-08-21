from enum import Enum
from typing import TYPE_CHECKING, Any, Optional, Union

from faststream.exceptions import SetupError
from faststream.response.publish_type import PublishType
from faststream.response.response import BatchPublishCommand, PublishCommand, Response
from typing_extensions import override

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from faststream._internal.basic_types import SendableMessage

INCORRECT_SETUP_MSG = "Specify exactly one of `queue` or `topic` to publish to."


class DestinationType(str, Enum):
    Queue = "queue"
    Topic = "topic"


class ServiceBusResponse(Response):
    """Return this from a handler to control how the reply is published."""

    def __init__(
        self,
        body: Optional["SendableMessage"] = None,
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        subject: str | None = None,
        session_id: str | None = None,
        time_to_live: Optional["timedelta"] = None,
        scheduled_enqueue_time: Optional["datetime"] = None,
    ) -> None:
        super().__init__(
            body=body,
            headers=headers,
            correlation_id=correlation_id,
        )
        self.subject = subject
        self.session_id = session_id
        self.time_to_live = time_to_live
        self.scheduled_enqueue_time = scheduled_enqueue_time

    @override
    def as_publish_command(self) -> "ServiceBusPublishCommand":
        return ServiceBusPublishCommand(
            self.body,
            headers=self.headers,
            correlation_id=self.correlation_id,
            subject=self.subject,
            session_id=self.session_id,
            time_to_live=self.time_to_live,
            scheduled_enqueue_time=self.scheduled_enqueue_time,
            _publish_type=PublishType.PUBLISH,
            # replaced by the reply sender
            queue="fake-queue",
        )


class ServiceBusPublishCommand(BatchPublishCommand):
    destination_type: DestinationType

    def __init__(
        self,
        message: "SendableMessage",
        /,
        *messages: "SendableMessage",
        _publish_type: "PublishType",
        queue: str | None = None,
        topic: str | None = None,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        reply_to: str = "",
        subject: str | None = None,
        session_id: str | None = None,
        partition_key: str | None = None,
        time_to_live: Optional["timedelta"] = None,
        scheduled_enqueue_time: Optional["datetime"] = None,
        timeout: float | None = 30.0,
    ) -> None:
        super().__init__(
            message,
            *messages,
            _publish_type=_publish_type,
            correlation_id=correlation_id,
            reply_to=reply_to,
            destination="",
            headers=headers,
        )

        self.set_destination(queue=queue, topic=topic)

        self.message_id = message_id
        self.subject = subject
        self.session_id = session_id
        self.partition_key = partition_key
        self.time_to_live = time_to_live
        self.scheduled_enqueue_time = scheduled_enqueue_time

        # request option
        self.timeout = timeout

    def set_destination(
        self,
        *,
        queue: str | None = None,
        topic: str | None = None,
    ) -> None:
        if (queue is None) == (topic is None):
            raise SetupError(INCORRECT_SETUP_MSG)

        if queue is not None:
            self.destination_type = DestinationType.Queue
            self.destination = queue
        else:
            assert topic is not None
            self.destination_type = DestinationType.Topic
            self.destination = topic

    @classmethod
    def from_cmd(
        cls,
        cmd: Union["PublishCommand", "ServiceBusPublishCommand"],
        *,
        batch: bool = False,
    ) -> "ServiceBusPublishCommand":
        if isinstance(cmd, ServiceBusPublishCommand):
            return cmd

        body, extra_bodies = cls._parse_bodies(cmd.body, batch=batch)

        return cls(
            body,
            *extra_bodies,
            queue=cmd.destination,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            _publish_type=cmd.publish_type,
        )
