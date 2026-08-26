import logging
from typing import TYPE_CHECKING, Any

from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus.exceptions import (
    MessageAlreadySettled,
    MessageLockLostError,
    SessionLockLostError,
)
from faststream.message import StreamMessage

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from azure.servicebus.aio import ServiceBusReceiver

    from faststream_azure_servicebus.configs.broker import ServiceBusBrokerConfig
    from faststream_azure_servicebus.schemas import Destination


# Settling a message whose lock has expired is not an application error: the broker
# has already released it and will redeliver. Swallowing these keeps a handler that
# simply took too long from also killing the consume loop.
LOST_LOCK_ERRORS = (MessageLockLostError, SessionLockLostError, MessageAlreadySettled)


class ServiceBusMessage(StreamMessage[ServiceBusReceivedMessage]):
    """A received Service Bus message.

    Settlement maps onto Service Bus' peek-lock verbs:

    | FastStream | Service Bus                                                        |
    |------------|-------------------------------------------------------------------|
    | `ack()`    | complete — removed from the entity                                 |
    | `nack()`   | abandon — released for redelivery, `delivery_count` increments     |
    | `reject()` | dead-letter                                                        |

    Messages received in `RECEIVE_AND_DELETE` mode hold no lock and cannot be
    settled, so the settlement calls become no-ops.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.destination: Destination | None = None
        self._receiver: ServiceBusReceiver | None = None
        self._logger_state: Any = None

    def _attach_destination(self, destination: "Destination") -> None:
        """Record the queue or subscription that delivered the message."""
        self.destination = destination

    def _attach_receiver(
        self,
        receiver: "ServiceBusReceiver",
        config: "ServiceBusBrokerConfig",
    ) -> None:
        """Give the message the receiver that holds its lock."""
        self._receiver = receiver
        self._logger_state = config.logger

    @property
    def delivery_count(self) -> int | None:
        """How many times the broker has delivered this message."""
        return self.raw_message.delivery_count

    async def ack(self) -> None:
        """Complete the message, removing it from the entity."""
        settled = self.committed is not None
        await super().ack()

        if settled or (receiver := self._receiver) is None:
            return

        await self._settle("complete", receiver.complete_message(self.raw_message))

    async def nack(self) -> None:
        """Abandon the message, releasing the lock for immediate redelivery."""
        settled = self.committed is not None
        await super().nack()

        if settled or (receiver := self._receiver) is None:
            return

        await self._settle("abandon", receiver.abandon_message(self.raw_message))

    async def reject(
        self,
        reason: str | None = None,
        description: str | None = None,
    ) -> None:
        """Dead-letter the message."""
        settled = self.committed is not None
        await super().reject()

        if settled or (receiver := self._receiver) is None:
            return

        await self._settle(
            "dead-letter",
            receiver.dead_letter_message(
                self.raw_message,
                reason=reason,
                error_description=description,
            ),
        )

    async def defer(self) -> None:
        """Defer the message.

        A deferred message can only be received again by sequence number, via
        `ServiceBusReceiver.receive_deferred_messages`.
        """
        if (receiver := self._receiver) is None:
            return

        await self._settle("defer", receiver.defer_message(self.raw_message))

    async def renew_lock(self) -> None:
        """Extend this message's lock by the entity's lock duration."""
        if (receiver := self._receiver) is None:
            return

        await receiver.renew_message_lock(self.raw_message)

    async def _settle(self, verb: str, action: "Coroutine[Any, Any, None]") -> None:
        try:
            await action
        except LOST_LOCK_ERRORS as exc:
            if self._logger_state is not None:
                self._logger_state.log(
                    f"Could not {verb} message {self.message_id}: "
                    f"{type(exc).__name__}. The lock is gone, so the broker will "
                    f"redeliver it.",
                    log_level=logging.WARNING,
                )
