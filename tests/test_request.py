from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from faststream.exceptions import IncorrectState

from faststream_azure_servicebus import ServiceBusBroker
from faststream_azure_servicebus.annotations import ServiceBusMessage

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from azure.servicebus.aio import ServiceBusClient

pytestmark = [pytest.mark.asyncio, pytest.mark.connected]

TIMEOUT = 10


@pytest_asyncio.fixture
async def request_broker(
    servicebus_connection_string: str,
) -> AsyncGenerator[ServiceBusBroker, None]:
    broker = ServiceBusBroker(servicebus_connection_string)
    async with broker:
        yield broker


async def test_queue_request_reply_preserves_protocol_properties(
    request_broker: ServiceBusBroker,
    queue: str,
    reply_queue: str,
) -> None:
    properties: dict[str, Any] = {}

    @request_broker.subscriber(queue)
    async def handler(
        body: dict[str, int],
        msg: ServiceBusMessage,
    ) -> dict[str, int]:
        properties.update(
            message_id=msg.message_id,
            correlation_id=msg.correlation_id,
            reply_to=msg.reply_to,
        )
        return {"answer": body["value"] * 2}

    await request_broker.start()
    response = await request_broker.request(
        {"value": 21},
        queue,
        reply_to=reply_queue,
        timeout=TIMEOUT,
        message_id="request-message",
        correlation_id="request-correlation",
    )

    assert await response.decode() == {"answer": 42}
    assert response.correlation_id == "request-correlation"
    assert properties == {
        "message_id": "request-message",
        "correlation_id": "request-correlation",
        "reply_to": reply_queue,
    }


async def test_concurrent_requests_are_correlated(
    request_broker: ServiceBusBroker,
    queue: str,
    reply_queue: str,
) -> None:
    @request_broker.subscriber(queue, max_workers=5, prefetch_count=5)
    async def handler(body: dict[str, int]) -> dict[str, int]:
        await asyncio.sleep((5 - body["value"]) / 100)
        return {"value": body["value"]}

    await request_broker.start()
    responses = await asyncio.gather(
        *(
            request_broker.request(
                {"value": value},
                queue,
                reply_to=reply_queue,
                timeout=TIMEOUT,
                correlation_id=f"request-{value}",
            )
            for value in range(5)
        )
    )

    assert [await response.decode() for response in responses] == [
        {"value": value} for value in range(5)
    ]
    assert [response.correlation_id for response in responses] == [
        f"request-{value}" for value in range(5)
    ]


async def test_request_supports_topic_destinations(
    request_broker: ServiceBusBroker,
    topic: str,
    reply_queue: str,
) -> None:
    @request_broker.subscriber(topic=topic, subscription="sub-0")
    async def handler(body: dict[str, int]) -> dict[str, int]:
        return {"answer": body["value"] + 1}

    await request_broker.start()
    response = await request_broker.request(
        {"value": 41},
        topic=topic,
        reply_to=reply_queue,
        timeout=TIMEOUT,
    )

    assert await response.decode() == {"answer": 42}


async def test_timeout_removes_waiter_and_late_reply_is_discarded(
    request_broker: ServiceBusBroker,
    queue: str,
    reply_queue: str,
) -> None:
    first_finished = asyncio.Event()

    @request_broker.subscriber(queue)
    async def handler(body: dict[str, Any]) -> dict[str, Any]:
        if body["slow"]:
            await asyncio.sleep(0.5)
            first_finished.set()
        return {"value": body["value"]}

    await request_broker.start()
    warmup = await request_broker.request(
        {"value": "warmup", "slow": False},
        queue,
        reply_to=reply_queue,
        timeout=TIMEOUT,
    )
    assert await warmup.decode() == {"value": "warmup"}

    with pytest.raises(TimeoutError):
        await request_broker.request(
            {"value": "late", "slow": True},
            queue,
            reply_to=reply_queue,
            timeout=0.1,
            correlation_id="timed-out",
        )

    await asyncio.wait_for(first_finished.wait(), timeout=TIMEOUT)
    response = await request_broker.request(
        {"value": "current", "slow": False},
        queue,
        reply_to=reply_queue,
        timeout=TIMEOUT,
        correlation_id="current",
    )

    assert await response.decode() == {"value": "current"}
    assert request_broker.config.producer._reply_receivers[reply_queue].pending_count == 0


async def test_cancellation_removes_waiter(
    request_broker: ServiceBusBroker,
    queue: str,
    reply_queue: str,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    @request_broker.subscriber(queue)
    async def handler() -> None:
        started.set()
        await release.wait()
        finished.set()

    await request_broker.start()
    request = asyncio.create_task(
        request_broker.request(
            {},
            queue,
            reply_to=reply_queue,
            timeout=TIMEOUT,
            correlation_id="cancelled",
        )
    )
    await asyncio.wait_for(started.wait(), timeout=TIMEOUT)

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    assert request_broker.config.producer._reply_receivers[reply_queue].pending_count == 0

    release.set()
    await asyncio.wait_for(finished.wait(), timeout=TIMEOUT)


async def test_shutdown_fails_pending_request(
    request_broker: ServiceBusBroker,
    queue: str,
    reply_queue: str,
    raw_client: ServiceBusClient,
) -> None:
    await request_broker.start()
    request = asyncio.create_task(
        request_broker.request(
            {},
            queue,
            reply_to=reply_queue,
            timeout=TIMEOUT,
        )
    )

    async with raw_client.get_queue_receiver(
        queue_name=queue,
        max_wait_time=TIMEOUT,
    ) as receiver:
        (published,) = await receiver.receive_messages(
            max_message_count=1,
            max_wait_time=TIMEOUT,
        )
        await receiver.complete_message(published)

    assert reply_queue in request_broker.config.producer._reply_receivers
    assert request_broker.config.producer._reply_receivers[reply_queue].pending_count == 1
    await request_broker.stop()

    with pytest.raises(IncorrectState, match="stopped"):
        await request


async def test_request_reply_recovers_after_broker_restart(
    request_broker: ServiceBusBroker,
    queue: str,
    reply_queue: str,
) -> None:
    @request_broker.subscriber(queue)
    async def handler(body: dict[str, int]) -> dict[str, int]:
        return {"attempt": body["attempt"]}

    await request_broker.start()
    first = await request_broker.request(
        {"attempt": 1},
        queue,
        reply_to=reply_queue,
        timeout=TIMEOUT,
    )

    await request_broker.stop()
    await request_broker.start()
    second = await request_broker.request(
        {"attempt": 2},
        queue,
        reply_to=reply_queue,
        timeout=TIMEOUT,
    )

    assert await first.decode() == {"attempt": 1}
    assert await second.decode() == {"attempt": 2}
