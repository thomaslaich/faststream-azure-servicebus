from unittest.mock import AsyncMock

import pytest
from prometheus_client import CollectorRegistry

from faststream_azure_servicebus import ServiceBusBroker, TestServiceBusBroker
from faststream_azure_servicebus.annotations import ServiceBusMessage
from faststream_azure_servicebus.prometheus import ServiceBusPrometheusMiddleware


def _sample(
    registry: CollectorRegistry,
    name: str,
    labels: dict[str, str],
) -> float | None:
    return registry.get_sample_value(name, labels)


@pytest.mark.asyncio()
async def test_middleware_records_publish_and_consume_metrics() -> None:
    registry = CollectorRegistry()
    middleware = ServiceBusPrometheusMiddleware(
        registry=registry,
        app_name="orders",
    )
    broker = ServiceBusBroker(
        "Endpoint=sb://unused",
        middlewares=(middleware,),
    )

    @broker.subscriber(queue="input")
    async def handler() -> None: ...

    async with TestServiceBusBroker(broker):
        await broker.publish_batch("one", "two", queue="input")

    publish_labels = {
        "app_name": "orders",
        "broker": "servicebus",
        "destination": "input",
        "status": "success",
    }
    consume_labels = {
        "app_name": "orders",
        "broker": "servicebus",
        "handler": "input",
    }

    assert (
        _sample(
            registry,
            "faststream_published_messages_total",
            publish_labels,
        )
        == 2
    )
    assert (
        _sample(
            registry,
            "faststream_received_messages_total",
            consume_labels,
        )
        == 2
    )
    assert (
        _sample(
            registry,
            "faststream_received_messages_size_bytes_count",
            consume_labels,
        )
        == 2
    )
    assert (
        _sample(
            registry,
            "faststream_received_messages_in_process",
            consume_labels,
        )
        == 0
    )
    assert (
        _sample(
            registry,
            "faststream_received_processed_messages_total",
            {**consume_labels, "status": "acked"},
        )
        == 2
    )


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ("action", "status"), (("nack", "nacked"), ("reject", "rejected"))
)
async def test_middleware_records_manual_settlement_status(
    action: str,
    status: str,
) -> None:
    registry = CollectorRegistry()
    broker = ServiceBusBroker(
        "Endpoint=sb://unused",
        middlewares=(
            ServiceBusPrometheusMiddleware(registry=registry, app_name="orders"),
        ),
    )

    @broker.subscriber(queue="input")
    async def handler(message: ServiceBusMessage) -> None:
        settlement = getattr(message, action)
        await settlement()

    async with TestServiceBusBroker(broker):
        await broker.publish("message", queue="input")

    assert (
        _sample(
            registry,
            "faststream_received_processed_messages_total",
            {
                "app_name": "orders",
                "broker": "servicebus",
                "handler": "input",
                "status": status,
            },
        )
        == 1
    )


@pytest.mark.asyncio()
async def test_custom_labels_are_applied() -> None:
    registry = CollectorRegistry()
    broker = ServiceBusBroker(
        "Endpoint=sb://unused",
        middlewares=(
            ServiceBusPrometheusMiddleware(
                registry=registry,
                app_name="orders",
                custom_labels={"team": "empire", "kind": lambda _: "queue"},
            ),
        ),
    )

    @broker.subscriber(queue="input")
    async def handler() -> None: ...

    async with TestServiceBusBroker(broker):
        await broker.publish("message", queue="input", message_id="message-id")

    assert (
        _sample(
            registry,
            "faststream_received_messages_total",
            {
                "app_name": "orders",
                "broker": "servicebus",
                "handler": "input",
                "team": "empire",
                "kind": "queue",
            },
        )
        == 1
    )


@pytest.mark.asyncio()
async def test_middleware_records_publish_errors() -> None:
    registry = CollectorRegistry()
    broker = ServiceBusBroker(
        "Endpoint=sb://unused",
        middlewares=(
            ServiceBusPrometheusMiddleware(registry=registry, app_name="orders"),
        ),
    )

    async with TestServiceBusBroker(broker):
        broker.config.producer.publish = AsyncMock(side_effect=RuntimeError("failed"))
        with pytest.raises(RuntimeError, match="failed"):
            await broker.publish("message", queue="input")

    labels = {
        "app_name": "orders",
        "broker": "servicebus",
        "destination": "input",
    }
    assert (
        _sample(
            registry,
            "faststream_published_messages_total",
            {**labels, "status": "error"},
        )
        == 1
    )
    assert (
        _sample(
            registry,
            "faststream_published_messages_exceptions_total",
            {**labels, "exception_type": "RuntimeError"},
        )
        == 1
    )
