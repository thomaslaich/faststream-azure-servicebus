from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, NoReturn, Optional, Union

from faststream._internal.endpoint.publisher import PublisherUsecase
from faststream.exceptions import FeatureNotSupportedException
from faststream.message import gen_cor_id
from faststream.response.publish_type import PublishType
from typing_extensions import override

from faststream_azure_servicebus.response import (
    DestinationType,
    ServiceBusPublishCommand,
)

from .producer import REQUEST_NOT_SUPPORTED

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.endpoint.publisher import PublisherSpecification
    from faststream._internal.types import PublisherMiddleware
    from faststream.response.response import PublishCommand

    from .config import ServiceBusPublisherConfig


class ServiceBusPublisher(PublisherUsecase):
    """A persistent queue or topic publisher."""

    def __init__(
        self,
        config: "ServiceBusPublisherConfig",
        specification: "PublisherSpecification[Any, Any]",
    ) -> None:
        super().__init__(config, specification)

        self.destination_type = config.destination_type
        self._destination = config.destination
        self.headers = config.headers or {}
        self.reply_to = config.reply_to
        self.subject = config.subject
        self.session_id = config.session_id
        self.partition_key = config.partition_key
        self.time_to_live = config.time_to_live
        self.scheduled_enqueue_time = config.scheduled_enqueue_time

    @property
    def destination(self) -> str:
        return f"{self._outer_config.prefix}{self._destination}"

    def _default_destination(self) -> tuple[str | None, str | None]:
        if self.destination_type is DestinationType.Queue:
            return self.destination, None
        return None, self.destination

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        queue: str | None = None,
        *,
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
    ) -> None:
        """Publish using this publisher's destination and message defaults.

        Passing `queue` or `topic` overrides the configured destination for one
        call. Per-call headers override headers configured on the publisher.
        """
        if queue is None and topic is None:
            queue, topic = self._default_destination()

        cmd = ServiceBusPublishCommand(
            message,
            queue=queue,
            topic=topic,
            headers=self.headers | (headers or {}),
            correlation_id=correlation_id or gen_cor_id(),
            message_id=message_id,
            reply_to=reply_to or self.reply_to,
            subject=subject or self.subject,
            session_id=session_id or self.session_id,
            partition_key=partition_key or self.partition_key,
            time_to_live=time_to_live or self.time_to_live,
            scheduled_enqueue_time=(
                scheduled_enqueue_time or self.scheduled_enqueue_time
            ),
            _publish_type=PublishType.PUBLISH,
        )
        await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )

    @override
    async def _publish(
        self,
        cmd: Union["PublishCommand", "ServiceBusPublishCommand"],
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> None:
        """Publish a subscriber handler's return value."""
        cmd = ServiceBusPublishCommand.from_cmd(cmd)

        queue, topic = self._default_destination()
        cmd.set_destination(queue=queue, topic=topic)
        cmd.add_headers(self.headers, override=False)
        cmd.reply_to = cmd.reply_to or self.reply_to
        cmd.subject = cmd.subject or self.subject
        cmd.session_id = cmd.session_id or self.session_id
        cmd.partition_key = cmd.partition_key or self.partition_key
        cmd.time_to_live = cmd.time_to_live or self.time_to_live
        cmd.scheduled_enqueue_time = (
            cmd.scheduled_enqueue_time or self.scheduled_enqueue_time
        )

        await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )

    @override
    async def request(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise FeatureNotSupportedException(REQUEST_NOT_SUPPORTED)
