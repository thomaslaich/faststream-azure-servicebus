from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from azure.servicebus import ServiceBusReceiveMode
from faststream.response.publish_type import PublishType

from faststream_azure_servicebus import ServiceBusBroker, TestServiceBusBroker
from faststream_azure_servicebus.configs import ServiceBusConnectionState
from faststream_azure_servicebus.message import ServiceBusMessage
from faststream_azure_servicebus.publisher.producer import ServiceBusProducer
from faststream_azure_servicebus.response import ServiceBusPublishCommand
from faststream_azure_servicebus.testing import PatchedMessage, PatchedReceiver


@pytest.mark.asyncio()
async def test_connection_state_caches_and_closes_senders() -> None:
    sender = AsyncMock()
    client = MagicMock()
    client.get_queue_sender.return_value = sender
    client.close = AsyncMock()
    state = ServiceBusConnectionState(MagicMock(return_value=client))
    await state.connect()

    async with state.sender(
        ServiceBusPublishCommand(
            "", queue="q", _publish_type=PublishType.PUBLISH
        ).destination_type,
        "q",
    ) as first:
        pass
    async with state.sender(
        ServiceBusPublishCommand(
            "", queue="q", _publish_type=PublishType.PUBLISH
        ).destination_type,
        "q",
    ) as second:
        pass

    assert first is second is sender
    client.get_queue_sender.assert_called_once_with(queue_name="q")
    await state.disconnect()
    sender.close.assert_awaited_once()
    client.close.assert_awaited_once()


@pytest.mark.asyncio()
async def test_connection_state_topic_sender_and_probe_health() -> None:
    sender = AsyncMock()
    receiver = AsyncMock()
    receiver.__aenter__.return_value = receiver
    client = MagicMock()
    client.get_topic_sender.return_value = sender
    client.close = AsyncMock()
    state = ServiceBusConnectionState(MagicMock(return_value=client))
    await state.connect()
    state.register_probe(MagicMock(return_value=receiver))

    command = ServiceBusPublishCommand("", topic="t", _publish_type=PublishType.PUBLISH)
    async with state.sender(command.destination_type, "t"):
        pass

    assert await state.ping()
    receiver.peek_messages.assert_awaited_once_with(max_message_count=1)

    receiver.peek_messages.side_effect = RuntimeError
    assert not await state.ping()


@pytest.mark.asyncio()
async def test_producer_publish_and_batch_use_connection_sender() -> None:
    sender = AsyncMock()
    batch = MagicMock()
    batch.__len__.return_value = 1
    sender.create_message_batch.return_value = batch

    @asynccontextmanager
    async def sender_context(*args: Any) -> Any:
        yield sender

    connection = MagicMock()
    connection.sender = sender_context
    producer = ServiceBusProducer(connection=connection, parser=None, decoder=None)

    await producer.publish(
        ServiceBusPublishCommand({"id": 1}, queue="q", _publish_type=PublishType.PUBLISH)
    )
    await producer.publish_batch(
        ServiceBusPublishCommand(
            {"id": 1},
            {"id": 2},
            queue="q",
            _publish_type=PublishType.PUBLISH,
        )
    )

    assert sender.send_messages.await_count == 2
    assert batch.add_message.call_count == 2


@pytest.mark.asyncio()
async def test_broker_offline_lifecycle_publish_batch_and_ping() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused")
    broker.connect = AsyncMock()
    broker.config.disconnect = AsyncMock()
    broker.config.producer.publish_batch = AsyncMock()

    await broker.start()
    await broker.publish_batch({"id": 1}, {"id": 2}, queue="q")
    await broker.stop()

    broker.config.producer.publish_batch.assert_awaited_once()
    broker.config.disconnect.assert_awaited_once()
    assert not await broker.ping(timeout=0)


@pytest.mark.asyncio()
async def test_message_all_settlement_operations_are_observable() -> None:
    receiver = PatchedReceiver()

    async def make_message() -> tuple[ServiceBusMessage, PatchedMessage]:
        raw = PatchedMessage(body=(b"body",))
        message = ServiceBusMessage(raw_message=raw, body=b"body")
        message._attach_receiver(receiver, SimpleNamespace(logger=None))  # type: ignore[arg-type]
        return message, raw

    message, raw = await make_message()
    await message.nack()
    assert raw.abandoned

    message, raw = await make_message()
    await message.reject(reason="bad", description="invalid")
    assert (raw.dead_lettered, raw.dead_letter_reason, raw.dead_letter_description) == (
        True,
        "bad",
        "invalid",
    )

    message, raw = await make_message()
    await message.defer()
    await message.renew_lock()
    assert raw.deferred
    assert raw.lock_renewed


@pytest.mark.asyncio()
async def test_subscriber_offline_start_get_one_and_stop() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused")
    subscriber = broker.subscriber(
        queue="q",
        max_lock_renewal_duration=None,
    )
    raw = PatchedMessage(
        body=(b'{"id":1}',),
        content_type="application/json",
        correlation_id="correlation",
        message_id="message",
    )
    receiver = AsyncMock()
    receiver.receive_messages.return_value = [raw]
    subscriber._open_receiver = MagicMock(return_value=receiver)  # type: ignore[method-assign]
    broker._setup_logger()

    await subscriber.start()
    message = await subscriber.get_one(timeout=0.1)
    await subscriber.stop()

    assert message is not None
    assert await message.decode() == {"id": 1}
    receiver.close.assert_awaited_once()


@pytest.mark.asyncio()
async def test_test_broker_batch_and_unmatched_publisher() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused")
    publisher = broker.publisher(topic="unused")

    async with TestServiceBusBroker(broker):
        await broker.publish_batch({"id": 1}, {"id": 2}, queue="missing")
        await publisher.publish({"id": 3})
        publisher.mock.assert_called_once()


def test_receive_mode_is_peek_lock_by_default() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused")
    assert (
        broker.subscriber(queue="q").config.receive_mode
        is ServiceBusReceiveMode.PEEK_LOCK
    )
