import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream.exceptions import IncorrectState

from faststream_azure_servicebus.response import DestinationType

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from azure.servicebus.aio import (
        ServiceBusClient,
        ServiceBusReceiver,
        ServiceBusSender,
    )


NOT_CONNECTED_MSG = "Connection is not available yet. Please, connect the broker first."


@dataclass
class _SenderEntry:
    sender: "ServiceBusSender"
    # The SDK does not guarantee coroutine-safety for a sender, so concurrent
    # publishes to the same entity are serialised.
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class _ClosableClientFactory:
    async def close(self) -> None:
        raise NotImplementedError


class _DefaultAzureCredentialClientFactory(_ClosableClientFactory):
    """Create one owned credential per client lifecycle."""

    def __init__(
        self,
        fully_qualified_namespace: str,
        options: dict[str, Any],
    ) -> None:
        try:
            from azure.identity.aio import DefaultAzureCredential
        except ImportError as exc:
            msg = (
                "Managed identity requires azure-identity. Install "
                "`faststream-azure-servicebus[identity]`."
            )
            raise ImportError(msg) from exc

        self._credential_type = DefaultAzureCredential
        self._fully_qualified_namespace = fully_qualified_namespace
        self._options = options
        self._credential: Any = None

    def __call__(self) -> "ServiceBusClient":
        from azure.servicebus.aio import ServiceBusClient

        self._credential = self._credential_type()
        return ServiceBusClient(
            fully_qualified_namespace=self._fully_qualified_namespace,
            credential=self._credential,
            **self._options,
        )

    async def close(self) -> None:
        if self._credential is None:
            return

        try:
            await self._credential.close()
        finally:
            self._credential = None


class ServiceBusConnectionState:
    """Owns the `ServiceBusClient` and the senders derived from it."""

    def __init__(self, client_factory: "Callable[[], ServiceBusClient]") -> None:
        self._client_factory = client_factory

        self._client: ServiceBusClient | None = None
        self._connected = False

        self._senders: dict[tuple[DestinationType, str], _SenderEntry] = {}
        self._senders_lock = asyncio.Lock()

        # `ServiceBusClient` has no entity-independent health check, so `ping()`
        # borrows an entity that some endpoint has told us about.
        self._probes: list[Callable[[ServiceBusClient], ServiceBusReceiver]] = []

    @property
    def client(self) -> "ServiceBusClient":
        if self._client is None:
            raise IncorrectState(NOT_CONNECTED_MSG)

        return self._client

    def __bool__(self) -> bool:
        return self._connected

    def register_probe(
        self,
        factory: "Callable[[ServiceBusClient], ServiceBusReceiver]",
    ) -> None:
        """Offer an entity that `ping()` may open a receiver against."""
        self._probes.append(factory)

    async def connect(self) -> "ServiceBusClient":
        if self._client is None:
            # Constructing the client performs no I/O; AMQP links open lazily.
            self._client = self._client_factory()

        self._connected = True
        return self._client

    async def disconnect(self) -> None:
        async with self._senders_lock:
            for entry in self._senders.values():
                with suppress(Exception):
                    await entry.sender.close()
            self._senders.clear()

        if self._client is not None:
            with suppress(Exception):
                await self._client.close()

        if isinstance(self._client_factory, _ClosableClientFactory):
            with suppress(Exception):
                await self._client_factory.close()

        self._client = None
        self._connected = False

    @asynccontextmanager
    async def sender(
        self,
        destination_type: DestinationType,
        name: str,
    ) -> "AsyncIterator[ServiceBusSender]":
        """Yield a cached sender for an entity, with its lock held."""
        entry = await self._get_sender_entry(destination_type, name)
        async with entry.lock:
            yield entry.sender

    async def _get_sender_entry(
        self,
        destination_type: DestinationType,
        name: str,
    ) -> _SenderEntry:
        key = (destination_type, name)

        if (entry := self._senders.get(key)) is not None:
            return entry

        async with self._senders_lock:
            # Another task may have created it while we waited.
            if (entry := self._senders.get(key)) is not None:
                return entry

            client = self.client
            sender: ServiceBusSender = (
                client.get_queue_sender(queue_name=name)
                if destination_type is DestinationType.Queue
                else client.get_topic_sender(topic_name=name)
            )

            entry = self._senders[key] = _SenderEntry(sender=sender)
            return entry

    async def ping(self) -> bool:
        """Prove the connection works by peeking at a known entity.

        Falls back to reporting whether a client exists when no endpoint has
        registered a probe — Service Bus offers no namespace-level ping.
        """
        if self._client is None:
            return False

        if not self._probes:
            return self._connected

        try:
            async with self._probes[0](self._client) as receiver:
                await receiver.peek_messages(max_message_count=1)
        except Exception:  # noqa: BLE001
            return False

        return True

    def __repr__(self) -> str:
        state = "connected" if self._connected else "disconnected"
        return f"{type(self).__name__}({state}, senders={len(self._senders)})"


def build_client_factory(
    connection_string: str | None,
    fully_qualified_namespace: str | None,
    credential: Any,
    options: dict[str, Any],
) -> "Callable[[], ServiceBusClient]":
    """Return a factory for the client described by the broker's arguments."""
    from azure.servicebus.aio import ServiceBusClient

    if connection_string is not None and (
        fully_qualified_namespace is not None or credential is not None
    ):
        msg = (
            "A connection string cannot be combined with "
            "`fully_qualified_namespace` or `credential`."
        )
        raise IncorrectState(msg)

    if connection_string is not None:

        def from_connection_string() -> ServiceBusClient:
            return ServiceBusClient.from_connection_string(
                connection_string,
                **options,
            )

        return from_connection_string

    if fully_qualified_namespace is None:
        msg = "Provide either a connection string or `fully_qualified_namespace`."
        raise IncorrectState(msg)

    if credential is None:
        return _DefaultAzureCredentialClientFactory(
            fully_qualified_namespace,
            options,
        )

    def from_credential() -> ServiceBusClient:
        return ServiceBusClient(
            fully_qualified_namespace=fully_qualified_namespace,
            credential=credential,
            **options,
        )

    return from_credential
