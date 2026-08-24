from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from azure.servicebus.amqp import AmqpMessageBodyType
from azure.servicebus.exceptions import MessageAlreadySettled
from faststream.exceptions import IncorrectState, SetupError
from faststream.response.publish_type import PublishType
from faststream.specification import AsyncAPI

from faststream_azure_servicebus import (
    ServiceBusBroker,
    ServiceBusPublisher,
    ServiceBusPublisherArgs,
    ServiceBusRoute,
    ServiceBusRouter,
)
from faststream_azure_servicebus.broker.broker import _specification_url
from faststream_azure_servicebus.configs import (
    ServiceBusConnectionState,
    ServiceBusRouterConfig,
    build_client_factory,
)
from faststream_azure_servicebus.message import ServiceBusMessage
from faststream_azure_servicebus.parser import extract_body, normalize_headers
from faststream_azure_servicebus.publisher import reply as reply_module
from faststream_azure_servicebus.publisher.producer import ServiceBusProducer
from faststream_azure_servicebus.publisher.reply import ServiceBusReplyReceiver
from faststream_azure_servicebus.response import (
    DestinationType,
    ServiceBusPublishCommand,
    ServiceBusResponse,
)
from faststream_azure_servicebus.subscriber.factory import (
    _validate_input_for_misconfigure,
    resolve_destination,
)

if TYPE_CHECKING:
    pass


@pytest.mark.parametrize(
    ("connection_string", "namespace", "expected"),
    (
        (
            None,
            "example.servicebus.windows.net",
            "amqps://example.servicebus.windows.net",
        ),
        (None, None, "amqps://localhost"),
    ),
)
def test_specification_url_fallbacks(
    connection_string: str | None,
    namespace: str | None,
    expected: str,
) -> None:
    assert _specification_url(connection_string, namespace) == expected


def test_publish_destination_requires_exactly_one() -> None:
    kwargs: dict[str, Any] = {"_publish_type": PublishType.PUBLISH}

    with pytest.raises(SetupError, match="exactly one"):
        ServiceBusPublishCommand("body", **kwargs)

    with pytest.raises(SetupError, match="exactly one"):
        ServiceBusPublishCommand("body", queue="queue", topic="topic", **kwargs)

    command = ServiceBusPublishCommand("body", topic="topic", **kwargs)
    assert command.destination_type is DestinationType.Topic
    assert command.destination == "topic"


def test_publisher_destination_requires_exactly_one() -> None:
    broker = ServiceBusBroker("Endpoint=sb://localhost")

    with pytest.raises(SetupError, match="exactly one"):
        broker.publisher()

    with pytest.raises(SetupError, match="exactly one"):
        broker.publisher(queue="queue", topic="topic")


@pytest.mark.asyncio()
async def test_publisher_uses_defaults_and_allows_call_overrides() -> None:
    broker = ServiceBusBroker("Endpoint=sb://localhost")
    publisher = broker.publisher(
        topic="events",
        headers={"source": "publisher", "default": True},
        subject="event.created",
    )
    producer_publish = AsyncMock()
    broker.config.producer.publish = producer_publish

    await publisher.publish(
        {"id": 1},
        headers={"source": "call"},
        correlation_id="correlation-id",
    )

    command = producer_publish.await_args.args[0]
    assert isinstance(publisher, ServiceBusPublisher)
    assert broker.publishers == [publisher]
    assert command.destination_type is DestinationType.Topic
    assert command.destination == "events"
    assert command.headers == {"source": "call", "default": True}
    assert command.subject == "event.created"
    assert command.correlation_id == "correlation-id"


def test_publisher_decorator_contributes_asyncapi_payload() -> None:
    broker = ServiceBusBroker("Endpoint=sb://localhost")
    publisher = broker.publisher(
        queue="results",
        title="results-publisher",
        description="Published handler results.",
    )

    @publisher
    async def handle() -> dict[str, int]:
        return {"answer": 42}

    schema = publisher.schema()["results-publisher"]
    asyncapi = AsyncAPI(broker).to_specification()

    assert publisher in handle._publishers
    assert schema.description == "Published handler results."
    assert schema.operation.message.payload
    assert asyncapi.operations["results-publisher"].action.value == "send"


def test_response_builds_a_service_bus_command() -> None:
    response = ServiceBusResponse(
        {"answer": 42},
        headers={"source": "handler"},
        subject="answer",
    )

    command = response.as_publish_command()

    assert command.body == {"answer": 42}
    assert command.headers == {"source": "handler"}
    assert command.subject == "answer"


@pytest.mark.parametrize(
    ("queue", "topic", "subscription", "message"),
    (
        ("queue", "topic", None, "either"),
        (None, "topic", None, "needs a `subscription`"),
        (None, None, None, "either"),
    ),
)
def test_invalid_subscriber_destinations(
    queue: str | None,
    topic: str | None,
    subscription: str | None,
    message: str,
) -> None:
    with pytest.raises(SetupError, match=message):
        resolve_destination(queue, topic, subscription)


@pytest.mark.parametrize(
    ("prefetch_count", "max_workers", "message"),
    (
        (0, 1, "prefetch_count"),
        (1, 0, "max_workers"),
        (1, 2, "starves"),
    ),
)
def test_invalid_subscriber_worker_configuration(
    prefetch_count: int,
    max_workers: int,
    message: str,
) -> None:
    from faststream._internal.constants import EMPTY

    with pytest.raises(SetupError, match=message):
        _validate_input_for_misconfigure(
            ack_policy=EMPTY,
            prefetch_count=prefetch_count,
            max_workers=max_workers,
        )


@pytest.mark.parametrize(
    ("body", "expected"),
    (
        (None, b""),
        (b"bytes", b"bytes"),
        ("text", b"text"),
        ({"key": "value"}, b'{"key": "value"}'),
        (42, b"42"),
    ),
)
def test_extract_body_variants(body: Any, expected: bytes) -> None:
    message = SimpleNamespace(body=body)
    assert extract_body(message) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("body_type", "body", "expected"),
    (
        (AmqpMessageBodyType.VALUE, {"key": "value"}, b'{"key": "value"}'),
        (AmqpMessageBodyType.VALUE, [1, "two"], b'[1, "two"]'),
        (
            AmqpMessageBodyType.SEQUENCE,
            iter(([1, 2], ["three"])),
            b'[[1, 2], ["three"]]',
        ),
    ),
)
def test_extract_body_preserves_amqp_value_and_sequence_shapes(
    body_type: AmqpMessageBodyType,
    body: Any,
    expected: bytes,
) -> None:
    message = SimpleNamespace(body=body, body_type=body_type)

    assert extract_body(message) == expected  # type: ignore[arg-type]


def test_normalize_headers_preserves_non_utf8_bytes() -> None:
    assert normalize_headers({b"binary": b"\xff"}) == {"binary": b"\xff"}


def test_connection_state_requires_a_connection() -> None:
    state = ServiceBusConnectionState(MagicMock())

    with pytest.raises(IncorrectState, match="connect the broker first"):
        _ = state.client

    assert not state
    assert "disconnected" in repr(state)


@pytest.mark.asyncio()
async def test_connection_state_ping_without_a_probe() -> None:
    client = AsyncMock()
    state = ServiceBusConnectionState(MagicMock(return_value=client))

    await state.connect()

    assert state
    assert await state.ping()
    assert "connected" in repr(state)

    await state.disconnect()
    client.close.assert_awaited_once()


def test_client_factory_requires_complete_credentials() -> None:
    with pytest.raises(IncorrectState, match="Provide either"):
        build_client_factory(None, "example.servicebus.windows.net", None, {})


@pytest.mark.parametrize(
    ("namespace", "credential"),
    (("example.servicebus.windows.net", None), (None, object())),
)
def test_client_factory_rejects_mixed_authentication_modes(
    namespace: str | None,
    credential: object | None,
) -> None:
    with pytest.raises(IncorrectState, match="cannot be combined"):
        build_client_factory("Endpoint=sb://localhost", namespace, credential, {})


def test_client_factory_builds_a_credential_client() -> None:
    credential = object()

    with patch("azure.servicebus.aio.ServiceBusClient") as client_type:
        factory = build_client_factory(
            None,
            "example.servicebus.windows.net",
            credential,
            {"retry_total": 7},
        )
        client = factory()

    assert client is client_type.return_value
    client_type.assert_called_once_with(
        fully_qualified_namespace="example.servicebus.windows.net",
        credential=credential,
        retry_total=7,
    )


def test_router_config_has_no_connection() -> None:
    config = ServiceBusRouterConfig()

    with pytest.raises(IncorrectState):
        _ = config.connection


def test_nested_router_prefixes_apply_to_every_endpoint() -> None:
    leaf = ServiceBusRouter(prefix="leaf.")
    subscriber = leaf.subscriber(queue="input")
    publisher = leaf.publisher(topic="output")
    parent = ServiceBusRouter(prefix="parent.", routers=(leaf,))
    broker = ServiceBusBroker("Endpoint=sb://localhost", routers=(parent,))

    assert subscriber.destination.path == "parent.leaf.input"
    assert publisher.destination == "parent.leaf.output"
    assert subscriber in broker.subscribers
    assert publisher in broker.publishers


def test_router_options_are_composed_into_endpoints() -> None:
    dependency = MagicMock()
    middleware = MagicMock()
    codec = MagicMock()
    router = ServiceBusRouter(
        dependencies=(dependency,),
        middlewares=(middleware,),
        codec=codec,
        include_in_schema=False,
    )
    subscriber = router.subscriber(queue="input")
    publisher = router.publisher(queue="output")
    ServiceBusBroker("Endpoint=sb://localhost", routers=(router,))

    assert tuple(subscriber._outer_config.broker_dependencies) == (dependency,)
    assert subscriber._outer_config.broker_middlewares == [middleware]
    assert subscriber._outer_config.broker_codec is codec
    assert not subscriber.specification.include_in_schema
    assert not publisher.specification.include_in_schema


def test_router_rejects_a_foreign_registrator() -> None:
    broker = ServiceBusBroker("Endpoint=sb://localhost")

    with pytest.raises(SetupError, match="ServiceBusRegistrator"):
        broker.include_router(MagicMock())  # type: ignore[arg-type]


def test_delayed_route_registers_subscriber_and_publisher() -> None:
    async def handler(body: dict[str, int]) -> dict[str, int]:
        return {"answer": body["n"] * 2}

    route = ServiceBusRoute(
        handler,
        queue="input",
        publishers=(ServiceBusPublisherArgs(topic="output"),),
    )
    router = ServiceBusRouter(prefix="v1.", handlers=(route,))
    broker = ServiceBusBroker("Endpoint=sb://localhost", routers=(router,))
    asyncapi = AsyncAPI(broker).to_specification()

    assert [subscriber.destination.path for subscriber in broker.subscribers] == [
        "v1.input"
    ]
    assert [publisher.destination for publisher in broker.publishers] == ["v1.output"]
    assert set(asyncapi.channels) == {
        "v1.input:Handler",
        "v1.output:Publisher",
    }


def make_message() -> ServiceBusMessage:
    raw = MagicMock(delivery_count=0)
    return ServiceBusMessage(raw_message=raw, body=b"body")


@pytest.mark.asyncio()
async def test_unattached_message_operations_are_noops() -> None:
    message = make_message()

    await message.defer()
    await message.renew_lock()


@pytest.mark.asyncio()
async def test_message_defer_and_lock_renewal_use_the_receiver() -> None:
    message = make_message()
    receiver = AsyncMock()
    message._attach_receiver(receiver, SimpleNamespace(logger=None))  # type: ignore[arg-type]

    await message.defer()
    await message.renew_lock()

    receiver.defer_message.assert_awaited_once_with(message.raw_message)
    receiver.renew_message_lock.assert_awaited_once_with(message.raw_message)


@pytest.mark.asyncio()
async def test_lost_settlement_lock_is_logged() -> None:
    message = make_message()
    receiver = AsyncMock()
    receiver.complete_message.side_effect = MessageAlreadySettled(action="complete")
    logger = MagicMock()
    message._attach_receiver(receiver, SimpleNamespace(logger=logger))  # type: ignore[arg-type]

    await message.ack()

    logger.log.assert_called_once()


@pytest.mark.asyncio()
async def test_empty_producer_batch_is_a_noop() -> None:
    producer = object.__new__(ServiceBusProducer)
    command = MagicMock(batch_bodies=[])

    await producer.publish_batch(command)


@pytest.mark.asyncio()
async def test_producer_request_requires_a_reply_queue() -> None:
    producer = object.__new__(ServiceBusProducer)
    producer._reply_receiver = None

    with pytest.raises(SetupError, match="reply_queue"):
        await producer.request(MagicMock())


@pytest.mark.asyncio()
async def test_producer_request_registers_before_publish_and_cleans_up() -> None:
    producer = object.__new__(ServiceBusProducer)
    producer._connected = True
    dispatcher = MagicMock()
    dispatcher.start = AsyncMock()
    producer._reply_receiver = dispatcher

    reply = MagicMock()
    future = asyncio.get_running_loop().create_future()
    dispatcher.register.return_value = future

    async def publish(command: Any) -> None:
        dispatcher.register.assert_called_once_with("correlation")
        future.set_result(reply)

    producer.publish = AsyncMock(side_effect=publish)  # type: ignore[method-assign]
    command = MagicMock(correlation_id="correlation", timeout=1)

    assert await producer.request(command) is reply
    dispatcher.start.assert_awaited_once()
    dispatcher.unregister.assert_called_once_with("correlation")


@pytest.mark.asyncio()
async def test_producer_disconnect_stops_reply_receiver() -> None:
    producer = object.__new__(ServiceBusProducer)
    producer._connected = True
    dispatcher = MagicMock()
    dispatcher.stop = AsyncMock()
    producer._reply_receiver = dispatcher

    await producer.disconnect()

    assert producer._connected is False
    dispatcher.stop.assert_awaited_once()


@pytest.mark.asyncio()
async def test_reply_receiver_routes_by_correlation_and_discards_late_replies() -> None:
    receiver = AsyncMock()
    connection = MagicMock()
    dispatcher = ServiceBusReplyReceiver(connection, "replies")
    dispatcher._receiver = receiver

    first = dispatcher.register("first")
    second = dispatcher.register("second")
    second_reply = MagicMock(correlation_id="second")
    first_reply = MagicMock(correlation_id="first")
    late_reply = MagicMock(correlation_id="late")

    await dispatcher._dispatch(second_reply)
    await dispatcher._dispatch(late_reply)
    await dispatcher._dispatch(first_reply)

    assert await first is first_reply
    assert await second is second_reply
    assert dispatcher.pending_count == 0
    assert receiver.complete_message.await_count == 3


@pytest.mark.asyncio()
async def test_reply_receiver_rejects_duplicate_waiters() -> None:
    dispatcher = ServiceBusReplyReceiver(MagicMock(), "replies")
    pending = dispatcher.register("duplicate")

    with pytest.raises(SetupError, match="already waiting"):
        dispatcher.register("duplicate")

    await dispatcher.stop()
    with pytest.raises(IncorrectState):
        await pending


@pytest.mark.asyncio()
async def test_reply_receiver_shutdown_fails_pending_waiters() -> None:
    dispatcher = ServiceBusReplyReceiver(MagicMock(), "replies")
    pending = dispatcher.register("pending")

    await dispatcher.stop()

    with pytest.raises(IncorrectState, match="stopped"):
        await pending
    assert dispatcher.pending_count == 0


@pytest.mark.asyncio()
async def test_reply_receiver_recovers_its_link(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reply_module, "REPLY_RECEIVE_BACKOFF_SECONDS", 0)

    broken_receiver = AsyncMock()
    broken_receiver.receive_messages.side_effect = RuntimeError("link failed")

    reply = MagicMock(correlation_id="request")
    recovered_receiver = AsyncMock()
    delivered = False

    async def receive_after_recovery(**kwargs: Any) -> list[Any]:
        nonlocal delivered
        if not delivered:
            delivered = True
            return [reply]
        await asyncio.Event().wait()
        return []

    recovered_receiver.receive_messages.side_effect = receive_after_recovery

    connection = MagicMock()
    connection.client.get_queue_receiver.side_effect = (
        broken_receiver,
        recovered_receiver,
    )
    dispatcher = ServiceBusReplyReceiver(connection, "replies")
    pending = dispatcher.register("request")

    await dispatcher.start()
    assert await asyncio.wait_for(pending, timeout=1) is reply
    await dispatcher.stop()

    broken_receiver.close.assert_awaited_once()
    recovered_receiver.complete_message.assert_awaited_once_with(reply)


class FakeBatch:
    def __init__(self) -> None:
        self.messages: list[Any] = []

    def add_message(self, message: Any) -> None:
        if self.messages:
            raise ValueError
        self.messages.append(message)

    def __len__(self) -> int:
        return len(self.messages)


@pytest.mark.asyncio()
async def test_producer_splits_full_batches() -> None:
    batches: list[FakeBatch] = []

    def new_batch() -> FakeBatch:
        batch = FakeBatch()
        batches.append(batch)
        return batch

    sender = MagicMock()
    sender.create_message_batch = AsyncMock(side_effect=new_batch)
    messages = [MagicMock(), MagicMock()]

    packed = await ServiceBusProducer._pack(sender, messages)

    assert packed == batches
    assert [batch.messages for batch in packed] == [[messages[0]], [messages[1]]]


@pytest.mark.asyncio()
async def test_producer_rejects_a_message_larger_than_an_empty_batch() -> None:
    batch = MagicMock()
    batch.__len__.return_value = 0
    batch.add_message.side_effect = ValueError("message is too large")
    sender = MagicMock()
    sender.create_message_batch = AsyncMock(return_value=batch)

    with pytest.raises(ValueError, match="too large"):
        await ServiceBusProducer._pack(sender, [MagicMock()])
