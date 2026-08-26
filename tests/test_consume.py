from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from azure.servicebus import ServiceBusSubQueue
from faststream import AckPolicy

from faststream_azure_servicebus import (
    ServiceBusBroker,
    ServiceBusPublisherArgs,
    ServiceBusRoute,
    ServiceBusRouter,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from azure.servicebus.aio import ServiceBusClient

pytestmark = [pytest.mark.asyncio, pytest.mark.connected]

TIMEOUT = 20

UNEXPECTED_SETTLEMENT_LOGS = (
    "Message fetch error",
    "Could not complete",
    "Could not abandon",
    "Could not dead-letter",
    "not supported in 'RECEIVE_AND_DELETE'",
)


def assert_no_unexpected_settlement_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    assert not any(message in caplog.text for message in UNEXPECTED_SETTLEMENT_LOGS)


@pytest_asyncio.fixture
async def broker(
    servicebus_connection_string: str,
) -> AsyncGenerator[ServiceBusBroker, None]:
    broker = ServiceBusBroker(servicebus_connection_string)
    try:
        yield broker
    finally:
        await broker.stop()


async def dead_letter_messages(
    client: ServiceBusClient,
    queue: str,
    *,
    timeout: float = 10,
) -> list[Any]:
    async with client.get_queue_receiver(
        queue_name=queue,
        sub_queue=ServiceBusSubQueue.DEAD_LETTER,
        max_wait_time=timeout,
    ) as receiver:
        return await receiver.receive_messages(
            max_message_count=10,
            max_wait_time=timeout,
        )


async def remaining_messages(
    client: ServiceBusClient,
    queue: str,
    *,
    timeout: float = 5,
) -> list[Any]:
    async with client.get_queue_receiver(
        queue_name=queue,
        max_wait_time=timeout,
    ) as receiver:
        return await receiver.receive_messages(
            max_message_count=10,
            max_wait_time=timeout,
        )


async def test_consume_from_queue(
    broker: ServiceBusBroker,
    queue: str,
    event: asyncio.Event,
) -> None:
    seen: list[Any] = []

    @broker.subscriber(queue)
    async def handler(body: dict[str, Any]) -> None:
        seen.append(body)
        event.set()

    await broker.start()
    await broker.publish({"hello": "world"}, queue=queue)

    await asyncio.wait_for(event.wait(), timeout=TIMEOUT)

    assert seen == [{"hello": "world"}]


async def test_consume_exposes_message_metadata(
    broker: ServiceBusBroker,
    queue: str,
    event: asyncio.Event,
) -> None:
    from faststream_azure_servicebus.annotations import ServiceBusMessage

    captured: dict[str, Any] = {}

    @broker.subscriber(queue)
    async def handler(body: dict[str, Any], msg: ServiceBusMessage) -> None:
        captured["headers"] = dict(msg.headers)
        captured["correlation_id"] = msg.correlation_id
        captured["delivery_count"] = msg.delivery_count
        captured["subject"] = msg.raw_message.subject
        event.set()

    await broker.start()
    await broker.publish(
        {"a": 1},
        queue=queue,
        headers={"x-tenant": "acme"},
        subject="thing.created",
    )

    await asyncio.wait_for(event.wait(), timeout=TIMEOUT)

    assert captured["headers"]["x-tenant"] == "acme"
    assert captured["correlation_id"]
    # 0 on first delivery; it counts *re*deliveries.
    assert captured["delivery_count"] == 0
    assert captured["subject"] == "thing.created"


async def test_successful_handler_completes_the_message(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
    event: asyncio.Event,
) -> None:
    @broker.subscriber(queue)
    async def handler(body: dict[str, Any]) -> None:
        event.set()

    await broker.start()
    await broker.publish({"a": 1}, queue=queue)
    await asyncio.wait_for(event.wait(), timeout=TIMEOUT)

    await broker.stop()

    assert await remaining_messages(raw_client, queue) == []


async def test_reject_on_error_dead_letters(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
    event: asyncio.Event,
    caplog: pytest.LogCaptureFixture,
) -> None:
    @broker.subscriber(queue)  # REJECT_ON_ERROR is the default
    async def handler(body: dict[str, Any]) -> None:
        event.set()
        msg = "handler blew up"
        raise ValueError(msg)

    await broker.start()
    await broker.publish({"a": 1}, queue=queue)
    await asyncio.wait_for(event.wait(), timeout=TIMEOUT)

    await broker.stop()

    dead_lettered = await dead_letter_messages(raw_client, queue)
    assert len(dead_lettered) == 1
    assert await remaining_messages(raw_client, queue) == []
    assert_no_unexpected_settlement_logs(caplog)


async def test_nack_on_error_redelivers(
    broker: ServiceBusBroker,
    queue: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    deliveries: list[int] = []
    twice = asyncio.Event()

    @broker.subscriber(queue, ack_policy=AckPolicy.NACK_ON_ERROR)
    async def handler(body: dict[str, Any]) -> None:
        deliveries.append(len(deliveries) + 1)
        if len(deliveries) >= 2:
            twice.set()
            return  # succeed the second time so the message settles
        msg = "not yet"
        raise ValueError(msg)

    await broker.start()
    await broker.publish({"a": 1}, queue=queue)

    await asyncio.wait_for(twice.wait(), timeout=TIMEOUT)

    assert len(deliveries) >= 2
    assert_no_unexpected_settlement_logs(caplog)


async def test_ack_policy_ack_settles_even_on_error(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
    event: asyncio.Event,
    caplog: pytest.LogCaptureFixture,
) -> None:
    @broker.subscriber(queue, ack_policy=AckPolicy.ACK)
    async def handler(body: dict[str, Any]) -> None:
        event.set()
        msg = "handler blew up"
        raise ValueError(msg)

    await broker.start()
    await broker.publish({"a": 1}, queue=queue)
    await asyncio.wait_for(event.wait(), timeout=TIMEOUT)

    await broker.stop()

    assert await remaining_messages(raw_client, queue) == []
    assert await dead_letter_messages(raw_client, queue, timeout=3) == []
    assert_no_unexpected_settlement_logs(caplog)


async def test_ack_first_uses_receive_and_delete(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
    event: asyncio.Event,
    caplog: pytest.LogCaptureFixture,
) -> None:
    @broker.subscriber(queue, ack_policy=AckPolicy.ACK_FIRST)
    async def handler(body: dict[str, Any]) -> None:
        event.set()
        msg = "handler blew up after the message was already gone"
        raise ValueError(msg)

    await broker.start()
    await broker.publish({"a": 1}, queue=queue)
    await asyncio.wait_for(event.wait(), timeout=TIMEOUT)

    await broker.stop()

    # RECEIVE_AND_DELETE: the message is gone on arrival, error or not.
    assert await remaining_messages(raw_client, queue) == []
    assert await dead_letter_messages(raw_client, queue, timeout=3) == []
    assert_no_unexpected_settlement_logs(caplog)


async def test_manual_ack_leaves_settlement_to_the_handler(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
    event: asyncio.Event,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from faststream_azure_servicebus.annotations import ServiceBusMessage

    @broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)
    async def handler(body: dict[str, Any], msg: ServiceBusMessage) -> None:
        await msg.ack()
        event.set()

    await broker.start()
    await broker.publish({"a": 1}, queue=queue)
    await asyncio.wait_for(event.wait(), timeout=TIMEOUT)

    await broker.stop()

    assert await remaining_messages(raw_client, queue) == []
    assert_no_unexpected_settlement_logs(caplog)


async def test_manual_reject_dead_letters(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
    event: asyncio.Event,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from faststream_azure_servicebus.annotations import ServiceBusMessage

    @broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)
    async def handler(body: dict[str, Any], msg: ServiceBusMessage) -> None:
        await msg.reject(reason="unwanted", description="not for us")
        event.set()

    await broker.start()
    await broker.publish({"a": 1}, queue=queue)
    await asyncio.wait_for(event.wait(), timeout=TIMEOUT)

    await broker.stop()

    dead_lettered = await dead_letter_messages(raw_client, queue)
    assert len(dead_lettered) == 1
    assert dead_lettered[0].dead_letter_reason == "unwanted"
    assert_no_unexpected_settlement_logs(caplog)


async def test_consume_from_topic_subscription(
    broker: ServiceBusBroker,
    topic: str,
    event: asyncio.Event,
) -> None:
    seen: list[Any] = []

    @broker.subscriber(topic=topic, subscription="sub-0")
    async def handler(body: dict[str, Any]) -> None:
        seen.append(body)
        event.set()

    await broker.start()
    await broker.publish({"event": "created"}, topic=topic)

    await asyncio.wait_for(event.wait(), timeout=TIMEOUT)

    assert seen == [{"event": "created"}]


async def test_concurrent_subscriber_processes_in_parallel(
    broker: ServiceBusBroker,
    queue: str,
) -> None:
    done = asyncio.Event()
    seen: list[int] = []

    @broker.subscriber(queue, max_workers=5, prefetch_count=5)
    async def handler(body: dict[str, Any]) -> None:
        await asyncio.sleep(0.3)
        seen.append(body["n"])
        if len(seen) == 5:
            done.set()

    await broker.start()
    await broker.publish_batch(*({"n": n} for n in range(5)), queue=queue)

    # Serially this would take 1.5s; concurrently it should be well under.
    await asyncio.wait_for(done.wait(), timeout=TIMEOUT)

    assert sorted(seen) == [0, 1, 2, 3, 4]


async def test_lock_is_renewed_while_a_slow_handler_runs(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    short_lock_queue: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    finished = asyncio.Event()

    @broker.subscriber(
        short_lock_queue,
        max_lock_renewal_duration=15,
    )
    async def handler() -> None:
        # The entity lock lasts five seconds. Completing after seven seconds is
        # only possible if AutoLockRenewer extends it while this handler runs.
        await asyncio.sleep(7)
        finished.set()

    await broker.start()
    await broker.publish({}, queue=short_lock_queue)
    await asyncio.wait_for(finished.wait(), timeout=TIMEOUT)
    await broker.stop()

    assert await remaining_messages(raw_client, short_lock_queue, timeout=2) == []
    assert "lock is gone" not in caplog.text


async def test_shutdown_waits_for_an_in_flight_handler(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    @broker.subscriber(queue)
    async def handler() -> None:
        started.set()
        await release.wait()
        finished.set()

    await broker.start()
    await broker.publish({}, queue=queue)
    await asyncio.wait_for(started.wait(), timeout=TIMEOUT)

    stopping = asyncio.create_task(broker.stop())
    await asyncio.sleep(0.2)
    assert not stopping.done()

    release.set()
    await asyncio.wait_for(stopping, timeout=TIMEOUT)

    assert finished.is_set()
    assert await remaining_messages(raw_client, queue, timeout=2) == []


async def test_reply_to_is_answered(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
    reply_queue: str,
    event: asyncio.Event,
) -> None:
    @broker.subscriber(queue)
    async def handler(body: dict[str, Any]) -> dict[str, Any]:
        event.set()
        return {"answer": body["n"] * 2}

    await broker.start()
    await broker.publish({"n": 21}, queue=queue, reply_to=reply_queue)
    await asyncio.wait_for(event.wait(), timeout=TIMEOUT)

    replies = await remaining_messages(raw_client, reply_queue, timeout=10)

    assert len(replies) == 1
    assert b"".join(replies[0].body) == b'{"answer":42}'


async def test_publisher_decorator_publishes_handler_result(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
    reply_queue: str,
) -> None:
    publisher = broker.publisher(queue=reply_queue, subject="answer")

    @broker.subscriber(queue)
    @publisher
    async def handler(body: dict[str, int]) -> dict[str, int]:
        return {"answer": body["n"] * 2}

    await broker.start()
    await broker.publish({"n": 21}, queue=queue)

    published = await remaining_messages(raw_client, reply_queue, timeout=TIMEOUT)

    assert len(published) == 1
    assert b"".join(published[0].body) == b'{"answer":42}'
    assert published[0].subject == "answer"


async def test_delayed_router_publishes_handler_result(
    broker: ServiceBusBroker,
    raw_client: ServiceBusClient,
    queue: str,
    reply_queue: str,
) -> None:
    async def handler(body: dict[str, int]) -> dict[str, int]:
        return {"answer": body["n"] * 2}

    route = ServiceBusRoute(
        handler,
        queue=queue,
        publishers=(ServiceBusPublisherArgs(queue=reply_queue, subject="router-answer"),),
    )
    broker.include_router(ServiceBusRouter(handlers=(route,)))

    await broker.start()
    await broker.publish({"n": 21}, queue=queue)

    published = await remaining_messages(raw_client, reply_queue, timeout=TIMEOUT)

    assert len(published) == 1
    assert b"".join(published[0].body) == b'{"answer":42}'
    assert published[0].subject == "router-answer"


async def test_get_one(broker: ServiceBusBroker, queue: str) -> None:
    subscriber = broker.subscriber(queue)

    await broker.start()
    await broker.publish({"a": 1}, queue=queue)

    message = await subscriber.get_one(timeout=TIMEOUT)

    assert message is not None
    assert await message.decode() == {"a": 1}
    await message.ack()


async def test_get_one_returns_none_when_empty(
    broker: ServiceBusBroker,
    queue: str,
) -> None:
    subscriber = broker.subscriber(queue)

    await broker.start()

    assert await subscriber.get_one(timeout=2) is None


async def test_iterator(broker: ServiceBusBroker, queue: str) -> None:
    subscriber = broker.subscriber(queue, prefetch_count=3)

    await broker.start()
    await broker.publish_batch({"n": 1}, {"n": 2}, queue=queue)

    seen = []
    async for message in subscriber:
        seen.append(await message.decode())
        await message.ack()
        if len(seen) == 2:
            break

    assert sorted(m["n"] for m in seen) == [1, 2]
