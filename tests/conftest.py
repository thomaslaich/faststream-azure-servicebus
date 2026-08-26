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

from tests.infra.containers import ServiceBusEmulator

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Iterator

    from xdist.workermanage import WorkerController

# Mirrors tests/infra/generate_config.py. The emulator cannot create entities at
# runtime, so tests lease names from the pool declared in its Config.json.
QUEUE_COUNT = 24
SHORT_LOCK_QUEUE_COUNT = 4
TOPIC_COUNT = 6
SUBSCRIPTIONS_PER_TOPIC = 2
ENTITY_READY_TIMEOUT = 60

_EMULATOR = pytest.StashKey[ServiceBusEmulator]()
_TEST_FAILED = pytest.StashKey[bool]()
_WORKER_CONNECTION_STRING = "servicebus_connection_string"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--servicebus-emulator",
        action="store_true",
        help="start a shared Service Bus emulator for xdist workers",
    )


def _external_connection_string() -> str | None:
    return os.environ.get("SERVICEBUS_CONNECTION_STRING")


def _start_emulator(config: pytest.Config) -> ServiceBusEmulator:
    emulator = ServiceBusEmulator().start()
    config.stash[_EMULATOR] = emulator
    return emulator


def _write_emulator_logs(config: pytest.Config, emulator: ServiceBusEmulator) -> None:
    logs = emulator.logs()
    reporter = config.pluginmanager.get_plugin("terminalreporter")
    if logs and reporter is not None:
        reporter.write_sep("=", "Service Bus testcontainer logs")
        reporter.write_line(logs)


def pytest_xdist_setupnodes(
    config: pytest.Config,
    specs: list[object],
) -> None:
    del specs
    if _external_connection_string() or not config.getoption("servicebus_emulator"):
        return
    _start_emulator(config)


def pytest_configure_node(node: WorkerController) -> None:
    emulator = node.config.stash.get(_EMULATOR, None)
    if emulator is not None:
        node.workerinput[_WORKER_CONNECTION_STRING] = emulator.connection_string


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item,
    call: pytest.CallInfo[None],
) -> Generator[None, None, None]:
    del call
    outcome = yield
    report = outcome.get_result()
    if report.failed:
        item.config.stash[_TEST_FAILED] = True


def pytest_testnodedown(node: WorkerController, error: object | None) -> None:
    worker_output = getattr(node, "workeroutput", {})
    if error is not None or worker_output.get("servicebus_test_failed", False):
        node.config.stash[_TEST_FAILED] = True


def pytest_sessionfinish(session: pytest.Session) -> None:
    config = session.config
    failed = config.stash.get(_TEST_FAILED, False) or session.exitstatus not in {
        pytest.ExitCode.OK,
        pytest.ExitCode.NO_TESTS_COLLECTED,
    }
    worker_output = getattr(config, "workeroutput", None)
    if worker_output is not None:
        worker_output["servicebus_test_failed"] = failed

    emulator = config.stash.get(_EMULATOR, None)
    if emulator is None:
        return
    if failed:
        _write_emulator_logs(config, emulator)
    emulator.stop()
    del config.stash[_EMULATOR]


@pytest.fixture(scope="session")
def servicebus_connection_string(
    request: pytest.FixtureRequest,
) -> Generator[str, None, None]:
    """Provide an external namespace or a pytest-owned emulator endpoint."""
    if connection_string := _external_connection_string():
        yield connection_string
        return

    worker_input = getattr(request.config, "workerinput", None)
    if worker_input is not None:
        connection_string = worker_input.get(_WORKER_CONNECTION_STRING)
        if connection_string is None:
            msg = "xdist controller did not start the Service Bus emulator"
            raise RuntimeError(msg)
        yield connection_string
        return

    emulator = _start_emulator(request.config)
    assert emulator.connection_string is not None
    try:
        yield emulator.connection_string
    finally:
        if request.config.stash.get(_TEST_FAILED, False):
            _write_emulator_logs(request.config, emulator)
        emulator.stop()
        del request.config.stash[_EMULATOR]


async def _wait_for_entity_pool(connection_string: str) -> None:
    """Wait until the emulator's final configured entity is queryable.

    The emulator health endpoint becomes ready before its asynchronous entity
    sync always finishes. Since entities are created in config order, opening
    the last subscription proves the complete queue/topic pool is available.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + ENTITY_READY_TIMEOUT

    while True:
        client = ServiceBusClient.from_connection_string(connection_string)
        try:
            async with (
                client,
                client.get_subscription_receiver(
                    topic_name=f"test-topic-{TOPIC_COUNT - 1:02d}",
                    subscription_name=f"sub-{SUBSCRIPTIONS_PER_TOPIC - 1}",
                    max_wait_time=1,
                ) as receiver,
            ):
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
        connection_string = request.getfixturevalue("servicebus_connection_string")
        await _wait_for_entity_pool(connection_string)
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
async def raw_client(
    servicebus_connection_string: str,
) -> AsyncGenerator[ServiceBusClient, None]:
    """A plain SDK client, for asserting on what actually reached the broker."""
    client = ServiceBusClient.from_connection_string(servicebus_connection_string)
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
