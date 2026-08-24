from __future__ import annotations

import asyncio
import os
from itertools import cycle
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from azure.servicebus import ServiceBusSubQueue
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus.exceptions import (
    MessagingEntityNotFoundError,
    ServiceBusConnectionError,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Iterator

# The emulator's well-known development credentials.
EMULATOR_CONNECTION_STRING = (
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"  # pragma: allowlist secret
)

# Mirrors tests/infra/generate_config.py. The emulator cannot create entities at
# runtime, so tests lease names from the pool declared in its Config.json.
QUEUE_COUNT = 24
SHORT_LOCK_QUEUE_COUNT = 4
TOPIC_COUNT = 6
SUBSCRIPTIONS_PER_TOPIC = 2
ENTITY_READY_TIMEOUT = 60


def connection_string() -> str:
    return os.environ.get("SERVICEBUS_CONNECTION_STRING", EMULATOR_CONNECTION_STRING)


async def _wait_for_entity_pool() -> None:
    """Wait until the emulator's final configured entity is queryable.

    The emulator health endpoint becomes ready before its asynchronous entity
    sync always finishes. Since entities are created in config order, opening
    the last subscription proves the complete queue/topic pool is available.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + ENTITY_READY_TIMEOUT

    while True:
        client = ServiceBusClient.from_connection_string(connection_string())
        try:
            async with client, client.get_subscription_receiver(
                topic_name=f"test-topic-{TOPIC_COUNT - 1:02d}",
                subscription_name=f"sub-{SUBSCRIPTIONS_PER_TOPIC - 1}",
                max_wait_time=1,
            ) as receiver:
                await receiver.peek_messages(max_message_count=1)
        except (MessagingEntityNotFoundError, ServiceBusConnectionError):
            if loop.time() >= deadline:
                raise
            await asyncio.sleep(0.5)
        else:
            return


@pytest_asyncio.fixture(scope="session", loop_scope="session", autouse=True)
async def emulator_entities_ready(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[None, None]:
    """Block connected workers until all fixed emulator entities exist."""
    if any(item.get_closest_marker("connected") for item in request.session.items):
        await _wait_for_entity_pool()

    yield


def _worker_slice(total: int, offset: int = 0) -> list[str]:
    """Give each xdist worker a disjoint slice of the entity pool.

    Without this, parallel workers would lease the same queue and see each
    other's messages.
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    index = int(worker.removeprefix("gw")) if worker.startswith("gw") else 0
    count = int(os.environ.get("PYTEST_XDIST_WORKER_COUNT", "1"))

    return [str(i) for i in range(offset, total) if (i - offset) % count == index % count]


@pytest.fixture(scope="session")
def queue_pool() -> Iterator[str]:
    # Queues 0..SHORT_LOCK_QUEUE_COUNT have a 5s lock and are reserved for
    # lock-expiry tests, so the general pool starts after them.
    names = [
        f"test-queue-{int(i):02d}"
        for i in _worker_slice(QUEUE_COUNT, offset=SHORT_LOCK_QUEUE_COUNT)
    ]
    assert names, "no queues left for this worker; grow the pool"
    return cycle(names)


@pytest.fixture(scope="session")
def short_lock_queue_pool() -> Iterator[str]:
    names = [f"test-queue-{int(i):02d}" for i in _worker_slice(SHORT_LOCK_QUEUE_COUNT)]
    return cycle(names or ["test-queue-00"])


@pytest.fixture(scope="session")
def topic_pool() -> Iterator[str]:
    names = [f"test-topic-{int(i):02d}" for i in _worker_slice(TOPIC_COUNT)]
    return cycle(names or ["test-topic-00"])


@pytest_asyncio.fixture
async def raw_client() -> AsyncGenerator[ServiceBusClient, None]:
    """A plain SDK client, for asserting on what actually reached the broker."""
    client = ServiceBusClient.from_connection_string(connection_string())
    async with client:
        yield client


async def _purge_queue(client: ServiceBusClient, name: str) -> None:
    """Empty a queue and its dead-letter sub-queue.

    The dead-letter queue matters: without draining it, a test asserting on
    dead-lettered messages sees every earlier run's failures too.
    """
    for sub_queue in (None, ServiceBusSubQueue.DEAD_LETTER):
        async with client.get_queue_receiver(
            queue_name=name,
            sub_queue=sub_queue,
            max_wait_time=1,
        ) as receiver:
            while messages := await receiver.receive_messages(
                max_message_count=100,
                max_wait_time=1,
            ):
                for message in messages:
                    await receiver.complete_message(message)


async def _purge_subscription(client: ServiceBusClient, topic: str, sub: str) -> None:
    async with client.get_subscription_receiver(
        topic_name=topic,
        subscription_name=sub,
        max_wait_time=1,
    ) as receiver:
        while messages := await receiver.receive_messages(
            max_message_count=100,
            max_wait_time=1,
        ):
            for message in messages:
                await receiver.complete_message(message)


@pytest_asyncio.fixture
async def queue(
    queue_pool: Iterator[str],
    raw_client: ServiceBusClient,
) -> AsyncGenerator[str, None]:
    """Lease a queue, emptied before and after the test."""
    name = next(queue_pool)
    await _purge_queue(raw_client, name)
    try:
        yield name
    finally:
        await _purge_queue(raw_client, name)


@pytest_asyncio.fixture
async def short_lock_queue(
    short_lock_queue_pool: Iterator[str],
    raw_client: ServiceBusClient,
) -> AsyncGenerator[str, None]:
    """Lease a queue whose five-second lock makes renewal observable."""
    name = next(short_lock_queue_pool)
    await _purge_queue(raw_client, name)
    try:
        yield name
    finally:
        await _purge_queue(raw_client, name)


@pytest_asyncio.fixture
async def reply_queue(
    queue_pool: Iterator[str],
    raw_client: ServiceBusClient,
) -> AsyncGenerator[str, None]:
    """A second leased queue, for `reply_to` destinations."""
    name = next(queue_pool)
    await _purge_queue(raw_client, name)
    try:
        yield name
    finally:
        await _purge_queue(raw_client, name)


@pytest_asyncio.fixture
async def topic(
    topic_pool: Iterator[str],
    raw_client: ServiceBusClient,
) -> AsyncGenerator[str, None]:
    """Lease a topic, with every subscription emptied before and after."""
    name = next(topic_pool)
    subscriptions = [f"sub-{i}" for i in range(SUBSCRIPTIONS_PER_TOPIC)]

    for sub in subscriptions:
        await _purge_subscription(raw_client, name, sub)
    try:
        yield name
    finally:
        for sub in subscriptions:
            await _purge_subscription(raw_client, name, sub)


@pytest.fixture()
def event() -> asyncio.Event:
    return asyncio.Event()


@pytest.fixture()
def mock() -> Generator[MagicMock, None, None]:
    m = MagicMock()
    yield m
    m.reset_mock()
