import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING

from faststream.exceptions import IncorrectState, SetupError

from faststream_azure_servicebus.message import LOST_LOCK_ERRORS

if TYPE_CHECKING:
    from azure.servicebus import ServiceBusReceivedMessage
    from azure.servicebus.aio import ServiceBusReceiver

    from faststream_azure_servicebus.configs.state import ServiceBusConnectionState


DUPLICATE_CORRELATION_ID = (
    "A request with correlation_id {correlation_id!r} is already waiting for a reply."
)
REQUESTS_STOPPED = "The broker stopped before the request received a reply."
BROKER_NOT_CONNECTED = "Request/reply requires a connected ServiceBusBroker."
REPLY_RECEIVE_BACKOFF_SECONDS = 1.0


class ServiceBusReplyReceiver:
    """Route replies from one queue to request futures by correlation id."""

    def __init__(
        self,
        connection: "ServiceBusConnectionState",
        reply_queue: str,
    ) -> None:
        self._connection = connection
        self.reply_queue = reply_queue

        self._receiver: ServiceBusReceiver | None = None
        self._task: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()
        self._running = False
        self._waiters: dict[
            str,
            asyncio.Future[ServiceBusReceivedMessage],
        ] = {}

    @property
    def pending_count(self) -> int:
        return len(self._waiters)

    async def start(self) -> None:
        """Start the shared receiver once, lazily on the first request."""
        async with self._start_lock:
            if self._task is not None and not self._task.done():
                return

            if not self._connection:
                raise IncorrectState(BROKER_NOT_CONNECTED)

            self._receiver = self._open_receiver()
            self._running = True
            self._task = asyncio.create_task(
                self._receive_replies(),
                name=f"servicebus-replies-{self.reply_queue}",
            )

    def register(
        self,
        correlation_id: str,
    ) -> "asyncio.Future[ServiceBusReceivedMessage]":
        if correlation_id in self._waiters:
            raise SetupError(
                DUPLICATE_CORRELATION_ID.format(correlation_id=correlation_id)
            )

        future = asyncio.get_running_loop().create_future()
        self._waiters[correlation_id] = future
        return future

    def unregister(self, correlation_id: str) -> None:
        self._waiters.pop(correlation_id, None)

    async def stop(self) -> None:
        """Stop receiving and fail every request that is still outstanding."""
        async with self._start_lock:
            self._running = False

            if self._task is not None:
                self._task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._task
                self._task = None

            await self._close_receiver()

            for future in self._waiters.values():
                if not future.done():
                    future.set_exception(IncorrectState(REQUESTS_STOPPED))
            self._waiters.clear()

    def _open_receiver(self) -> "ServiceBusReceiver":
        return self._connection.client.get_queue_receiver(
            queue_name=self.reply_queue,
            max_wait_time=1,
        )

    async def _close_receiver(self) -> None:
        if self._receiver is not None:
            with suppress(Exception):
                await self._receiver.close()
            self._receiver = None

    async def _receive_replies(self) -> None:
        try:
            while self._running:
                try:
                    if self._receiver is None:
                        self._receiver = self._open_receiver()

                    replies = await self._receiver.receive_messages(
                        max_message_count=100,
                        max_wait_time=1,
                    )
                    for reply in replies:
                        await self._dispatch(reply)

                except asyncio.CancelledError:  # noqa: PERF203
                    raise

                except Exception:  # noqa: BLE001 -- recover the AMQP link
                    await self._close_receiver()
                    if self._running:
                        await asyncio.sleep(REPLY_RECEIVE_BACKOFF_SECONDS)

        finally:
            self._running = False
            await self._close_receiver()

    async def _dispatch(self, reply: "ServiceBusReceivedMessage") -> None:
        assert self._receiver is not None

        # A lost lock means the reply is already gone, so it is still safe to
        # deliver it to its local waiter.
        with suppress(*LOST_LOCK_ERRORS):
            await self._receiver.complete_message(reply)

        correlation_id = str(reply.correlation_id) if reply.correlation_id else ""
        future = self._waiters.pop(correlation_id, None)

        # Unknown ids are late replies (or replies for another broker instance).
        # Completing them above keeps the shared queue from accumulating debris.
        if future is not None and not future.done():
            future.set_result(reply)
