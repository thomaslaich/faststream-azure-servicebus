from __future__ import annotations

import asyncio
import os
from pprint import pprint
from typing import TYPE_CHECKING
from uuid import uuid4

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from faststream_azure_servicebus import ServiceBusBroker
from faststream_azure_servicebus.annotations import (
    ServiceBusMessage,  # noqa: TC001 -- FastStream resolves this annotation at runtime
)
from faststream_azure_servicebus.opentelemetry import ServiceBusTelemetryMiddleware

if TYPE_CHECKING:
    from faststream.types import SendableMessage

EMULATOR_CONNECTION_STRING = (
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"  # pragma: allowlist secret
)
QUEUE = "example-queue"
GRAFANA_URL = "http://localhost:3000"


def create_telemetry() -> tuple[TracerProvider, MeterProvider]:
    resource = Resource.create({"service.name": "faststream-azure-servicebus-example"})
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=(PeriodicExportingMetricReader(OTLPMetricExporter()),),
    )
    return tracer_provider, meter_provider


def print_wire_message(msg: ServiceBusMessage) -> None:
    pprint(  # noqa: T203 -- intentionally show the complete wire message
        {
            "body": msg.body,
            "content_type": msg.content_type,
            "message_id": msg.message_id,
            "correlation_id": msg.correlation_id,
            "reply_to": msg.reply_to,
            "headers": msg.headers,
        },
        sort_dicts=False,
    )


async def main() -> None:
    connection_string = os.environ.get(
        "SERVICEBUS_CONNECTION_STRING",
        EMULATOR_CONNECTION_STRING,
    )
    tracer_provider, meter_provider = create_telemetry()
    broker = ServiceBusBroker(
        connection_string,
        middlewares=(
            ServiceBusTelemetryMiddleware(
                tracer_provider=tracer_provider,
                meter_provider=meter_provider,
            ),
        ),
    )
    publisher = broker.publisher(queue=QUEUE)
    consumed = asyncio.Event()
    payload: SendableMessage = {
        "id": uuid4().hex[:8],
        "message": "hello from FastStream",
    }

    @broker.subscriber(QUEUE)
    async def handle_message(
        body: dict[str, str],
        msg: ServiceBusMessage,
    ) -> None:
        print("wire message:")
        print_wire_message(msg)
        print(f"received: {body}")
        if body == payload:
            consumed.set()

    try:
        await broker.start()
        print(f"publishing: {payload}")
        await publisher.publish(payload)
        await asyncio.wait_for(consumed.wait(), timeout=15)
        print(f"traces and metrics: {GRAFANA_URL}/explore")
    finally:
        await broker.stop()
        tracer_provider.shutdown()
        meter_provider.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
