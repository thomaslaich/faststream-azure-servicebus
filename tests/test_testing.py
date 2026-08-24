import asyncio
from typing import Any

import pytest

from faststream_azure_servicebus import ServiceBusBroker, TestServiceBusBroker
from faststream_azure_servicebus.annotations import ServiceBusMessage


@pytest.mark.asyncio()
async def test_test_broker_routes_queue_messages_and_settles() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused")
    received: list[Any] = []

    @broker.subscriber(queue="orders")
    async def handler(body: dict[str, int], msg: ServiceBusMessage) -> None:
        received.append((body, msg.raw_message))

    async with TestServiceBusBroker(broker):
        await broker.publish({"id": 1}, queue="orders")

    body, raw = received[0]
    assert body == {"id": 1}
    assert raw.completed


@pytest.mark.asyncio()
async def test_test_broker_fans_topics_out_by_subscription() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused")
    seen: list[str] = []

    @broker.subscriber(topic="events", subscription="billing")
    async def billing() -> None:
        seen.append("billing")

    @broker.subscriber(topic="events", subscription="analytics")
    async def analytics() -> None:
        seen.append("analytics")

    async with TestServiceBusBroker(broker):
        await broker.publish({"id": 1}, topic="events")

    assert sorted(seen) == ["analytics", "billing"]


@pytest.mark.asyncio()
async def test_test_broker_routes_handler_replies() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused")
    replies: list[dict[str, int]] = []

    @broker.subscriber(queue="requests")
    async def request(body: dict[str, int]) -> dict[str, int]:
        return {"answer": body["value"] * 2}

    @broker.subscriber(queue="replies")
    async def reply(body: dict[str, int]) -> None:
        replies.append(body)

    async with TestServiceBusBroker(broker):
        await broker.publish({"value": 21}, queue="requests", reply_to="replies")

    assert replies == [{"answer": 42}]


@pytest.mark.asyncio()
async def test_test_broker_routes_publisher_decorators() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused")
    publisher = broker.publisher(queue="results")
    results: list[dict[str, int]] = []

    @broker.subscriber(queue="requests")
    @publisher
    async def request(body: dict[str, int]) -> dict[str, int]:
        return {"answer": body["value"] * 2}

    @broker.subscriber(queue="results")
    async def result(body: dict[str, int]) -> None:
        results.append(body)

    async with TestServiceBusBroker(broker):
        await broker.publish({"value": 21}, queue="requests")

    assert results == [{"answer": 42}]


@pytest.mark.asyncio()
async def test_test_broker_routes_between_multiple_brokers() -> None:
    producer_broker = ServiceBusBroker("Endpoint=sb://unused")
    consumer_broker = ServiceBusBroker("Endpoint=sb://unused")
    seen: list[dict[str, int]] = []

    @consumer_broker.subscriber(queue="shared")
    async def handler(body: dict[str, int]) -> None:
        seen.append(body)

    async with TestServiceBusBroker(producer_broker, consumer_broker):
        await producer_broker.publish({"id": 1}, queue="shared")

    assert seen == [{"id": 1}]


@pytest.mark.asyncio()
async def test_test_broker_request_reply_preserves_protocol_properties() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused", reply_queue="replies")
    properties: dict[str, Any] = {}

    @broker.subscriber(queue="requests")
    async def handler(body: dict[str, int], msg: ServiceBusMessage) -> dict[str, int]:
        properties.update(
            message_id=msg.message_id,
            correlation_id=msg.correlation_id,
            reply_to=msg.reply_to,
        )
        return {"answer": body["value"] * 2}

    async with TestServiceBusBroker(broker):
        response = await broker.request(
            {"value": 21},
            queue="requests",
            message_id="request-message",
            correlation_id="request-correlation",
        )

    assert await response.decode() == {"answer": 42}
    assert response.correlation_id == "request-correlation"
    assert properties == {
        "message_id": "request-message",
        "correlation_id": "request-correlation",
        "reply_to": "replies",
    }


@pytest.mark.asyncio()
async def test_test_broker_concurrent_requests_match_their_replies() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused", reply_queue="replies")

    @broker.subscriber(queue="requests")
    async def handler(body: dict[str, float]) -> dict[str, float]:
        await asyncio.sleep(body["delay"])
        return {"value": body["value"]}

    async with TestServiceBusBroker(broker):
        slow, fast = await asyncio.gather(
            broker.request(
                {"value": 1, "delay": 0.02},
                queue="requests",
                correlation_id="slow",
            ),
            broker.request(
                {"value": 2, "delay": 0.0},
                queue="requests",
                correlation_id="fast",
            ),
        )

    assert await slow.decode() == {"value": 1}
    assert slow.correlation_id == "slow"
    assert await fast.decode() == {"value": 2}
    assert fast.correlation_id == "fast"


@pytest.mark.asyncio()
async def test_test_broker_request_supports_topic_destinations() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused", reply_queue="replies")

    @broker.subscriber(topic="requests", subscription="worker")
    async def handler(body: dict[str, int]) -> dict[str, int]:
        return {"answer": body["value"] + 1}

    async with TestServiceBusBroker(broker):
        response = await broker.request({"value": 41}, topic="requests")

    assert await response.decode() == {"answer": 42}


@pytest.mark.asyncio()
async def test_test_broker_request_times_out() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused", reply_queue="replies")

    @broker.subscriber(queue="requests")
    async def handler() -> None:
        await asyncio.sleep(1)

    async with TestServiceBusBroker(broker):
        with pytest.raises(TimeoutError):
            await broker.request({}, queue="requests", timeout=0.01)
