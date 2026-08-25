from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind

from faststream_azure_servicebus import ServiceBusBroker, TestServiceBusBroker
from faststream_azure_servicebus.annotations import ServiceBusMessage
from faststream_azure_servicebus.message import ServiceBusMessage as Message
from faststream_azure_servicebus.opentelemetry import ServiceBusTelemetryMiddleware
from faststream_azure_servicebus.opentelemetry.provider import (
    MESSAGING_DESTINATION_SUBSCRIPTION_NAME,
    MESSAGING_SERVICEBUS_DELIVERY_COUNT,
    MESSAGING_SERVICEBUS_ENQUEUED_TIME,
    ServiceBusTelemetrySettingsProvider,
)
from faststream_azure_servicebus.schemas import SubscriptionDestination


def test_consume_attributes_include_service_bus_metadata() -> None:
    enqueued_time = datetime(2026, 8, 25, tzinfo=UTC)
    raw = SimpleNamespace(delivery_count=2, enqueued_time_utc=enqueued_time)
    message = Message(
        raw_message=raw,
        body=b"payload",
        message_id="message-id",
        correlation_id="correlation-id",
    )
    message._attach_destination(SubscriptionDestination("events", "worker"))

    attrs = ServiceBusTelemetrySettingsProvider().get_consume_attrs_from_message(
        message  # type: ignore[arg-type]
    )

    assert attrs == {
        SpanAttributes.MESSAGING_SYSTEM: "servicebus",
        SpanAttributes.MESSAGING_MESSAGE_ID: "message-id",
        SpanAttributes.MESSAGING_MESSAGE_CONVERSATION_ID: "correlation-id",
        SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: 7,
        MESSAGING_DESTINATION_PUBLISH_NAME: "events/worker",
        MESSAGING_DESTINATION_SUBSCRIPTION_NAME: "worker",
        MESSAGING_SERVICEBUS_DELIVERY_COUNT: 2,
        MESSAGING_SERVICEBUS_ENQUEUED_TIME: int(enqueued_time.timestamp()),
    }


@pytest.mark.asyncio()
async def test_middleware_emits_spans_metrics_and_trace_context() -> None:
    span_exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=(metric_reader,))
    middleware = ServiceBusTelemetryMiddleware(
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
    )
    broker = ServiceBusBroker(
        "Endpoint=sb://unused",
        middlewares=(middleware,),
    )
    received_headers: list[dict[str, Any]] = []

    @broker.subscriber(queue="input")
    async def handler(message: ServiceBusMessage) -> None:
        received_headers.append(message.headers)

    async with TestServiceBusBroker(broker):
        await broker.publish(
            "hello",
            queue="input",
            correlation_id="correlation-id",
            message_id="message-id",
        )

    spans = span_exporter.get_finished_spans()
    publish_span = next(span for span in spans if span.name == "input publish")
    process_span = next(span for span in spans if span.name == "input process")

    assert publish_span.kind is SpanKind.PRODUCER
    assert process_span.kind is SpanKind.CONSUMER
    assert process_span.context.trace_id == publish_span.context.trace_id
    assert process_span.attributes is not None
    assert process_span.attributes[SpanAttributes.MESSAGING_SYSTEM] == "servicebus"
    assert process_span.attributes[MESSAGING_DESTINATION_PUBLISH_NAME] == "input"
    assert received_headers[0]["traceparent"]

    metrics = metric_reader.get_metrics_data()
    metric_names = {
        metric.name
        for resource_metrics in metrics.resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
    }
    assert {
        "messaging.process.duration",
        "messaging.process.messages",
        "messaging.publish.duration",
        "messaging.publish.messages",
    } <= metric_names
