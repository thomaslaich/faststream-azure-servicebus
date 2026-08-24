from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from faststream_azure_servicebus import ServiceBusBroker
from tests.conftest import SUBSCRIPTIONS_PER_TOPIC, connection_string

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from azure.servicebus import ServiceBusReceivedMessage
    from azure.servicebus.aio import ServiceBusClient

pytestmark = [pytest.mark.asyncio, pytest.mark.connected]


@pytest_asyncio.fixture
async def broker() -> AsyncGenerator[ServiceBusBroker, None]:
    broker = ServiceBusBroker(connection_string())
    async with broker:
        yield broker


async def receive_from_queue(
    client: ServiceBusClient,
    name: str,
    *,
    count: int = 1,
    wait: float = 5,
) -> list[ServiceBusReceivedMessage]:
    async with client.get_queue_receiver(
        queue_name=name,
        max_wait_time=wait,
    ) as receiver:
        return await receiver.receive_messages(
            max_message_count=count,
            max_wait_time=wait,
        )


def body_of(message: ServiceBusReceivedMessage) -> bytes:
    return b"".join(message.body)


async def test_publish_to_queue(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
) -> None:
    await broker.publish(
        {"hello": "world"},
        queue=queue,
        headers={"x-source": "faststream"},
        subject="greeting",
    )

    (message,) = await receive_from_queue(raw_client, queue)

    assert body_of(message) == b'{"hello":"world"}'
    assert message.content_type == "application/json"
    assert message.subject == "greeting"
    assert message.correlation_id
    assert message.application_properties[b"x-source"] == b"faststream"


async def test_published_message_round_trips_through_the_parser(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
) -> None:
    await broker.publish(
        {"hello": "world"},
        queue=queue,
        headers={"x-source": "faststream"},
    )

    (message,) = await receive_from_queue(raw_client, queue)

    producer = broker.config.producer
    parsed = await producer._parser(message)
    decoded = await producer._decoder(parsed)

    assert decoded == {"hello": "world"}
    # AMQP hands back bytes keys and values; the parser normalises both.
    assert parsed.headers["x-source"] == "faststream"


@pytest.mark.parametrize(
    ("sent", "expected"),
    (
        pytest.param(b"raw-bytes", b"raw-bytes", id="bytes"),
        pytest.param("plain-text", b"plain-text", id="str"),
        pytest.param({"a": 1}, b'{"a":1}', id="dict"),
        pytest.param(None, b"", id="none"),
    ),
)
async def test_publish_body_types(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
    sent: object,
    expected: bytes,
) -> None:
    await broker.publish(sent, queue=queue)

    (message,) = await receive_from_queue(raw_client, queue)

    assert body_of(message) == expected


async def test_publish_batch(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
) -> None:
    await broker.publish_batch(
        {"n": 1},
        {"n": 2},
        {"n": 3},
        queue=queue,
        headers={"batch": "yes"},
    )

    messages = await receive_from_queue(raw_client, queue, count=10)

    assert len(messages) == 3
    assert sorted(body_of(m) for m in messages) == [b'{"n":1}', b'{"n":2}', b'{"n":3}']
    assert all(m.application_properties[b"batch"] == b"yes" for m in messages)


async def test_publish_batch_of_nothing_is_a_noop(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
) -> None:
    await broker.publish_batch(queue=queue)

    assert await receive_from_queue(raw_client, queue, count=5) == []


async def test_message_expires_after_its_time_to_live(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
) -> None:
    await broker.publish(
        {"expired": True},
        queue=queue,
        time_to_live=timedelta(seconds=1),
    )

    await asyncio.sleep(2)

    assert await receive_from_queue(raw_client, queue, count=5, wait=1) == []


async def test_scheduled_message_is_not_delivered_early(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
) -> None:
    enqueue_at = datetime.now(timezone.utc) + timedelta(seconds=3)
    await broker.publish(
        {"scheduled": True},
        queue=queue,
        scheduled_enqueue_time=enqueue_at,
    )

    assert await receive_from_queue(raw_client, queue, wait=0.5) == []

    await asyncio.sleep(3)
    (message,) = await receive_from_queue(raw_client, queue, wait=5)

    assert body_of(message) == b'{"scheduled":true}'


async def test_publish_to_topic_reaches_every_subscription(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    topic: str,
) -> None:
    await broker.publish({"event": "created"}, topic=topic)

    for index in range(SUBSCRIPTIONS_PER_TOPIC):
        async with raw_client.get_subscription_receiver(
            topic_name=topic,
            subscription_name=f"sub-{index}",
            max_wait_time=5,
        ) as receiver:
            messages = await receiver.receive_messages(
                max_message_count=5,
                max_wait_time=5,
            )

        assert len(messages) == 1, f"sub-{index} received {len(messages)}"
        assert body_of(messages[0]) == b'{"event":"created"}'


async def test_publish_without_a_destination_is_an_error(
    broker: ServiceBusBroker,
) -> None:
    from faststream.exceptions import SetupError

    with pytest.raises(SetupError):
        await broker.publish({"a": 1})


async def test_request_requires_reply_to(
    broker: ServiceBusBroker,
) -> None:
    from faststream.exceptions import SetupError

    with pytest.raises(SetupError, match="reply_to"):
        await broker.request({"a": 1}, queue="whatever", reply_to="")


async def test_ping(broker: ServiceBusBroker, queue: str) -> None:
    broker.config.broker_config.connection.register_probe(
        lambda client: client.get_queue_receiver(queue_name=queue, max_wait_time=2),
    )

    assert await broker.ping(timeout=10) is True


async def test_ping_without_a_connection() -> None:
    broker = ServiceBusBroker(connection_string())

    assert await broker.ping(timeout=1) is False


async def test_publish_reconnects_after_broker_restart(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
) -> None:
    await broker.start()
    await broker.publish({"attempt": 1}, queue=queue)
    (first,) = await receive_from_queue(raw_client, queue)

    await broker.stop()
    await broker.start()
    await broker.publish({"attempt": 2}, queue=queue)
    (second,) = await receive_from_queue(raw_client, queue)

    assert body_of(first) == b'{"attempt":1}'
    assert body_of(second) == b'{"attempt":2}'
