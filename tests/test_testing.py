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
