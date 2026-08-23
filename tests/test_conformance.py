from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from faststream import BaseMiddleware, Depends

from faststream_azure_servicebus import (
    ServiceBusBroker,
    ServiceBusResponse,
    TestServiceBusBroker,
)
from faststream_azure_servicebus.annotations import ServiceBusMessage


@pytest.mark.asyncio()
async def test_dependencies_run_and_inject_values() -> None:
    calls: list[str] = []

    async def broker_dependency() -> None:
        calls.append("broker")

    async def subscriber_dependency() -> None:
        calls.append("subscriber")

    async def injected_dependency() -> str:
        calls.append("injected")
        return "dependency-value"

    broker = ServiceBusBroker(
        "Endpoint=sb://unused",
        dependencies=(Depends(broker_dependency),),
    )
    seen: list[tuple[dict[str, int], str]] = []

    @broker.subscriber(
        queue="input",
        dependencies=(Depends(subscriber_dependency),),
    )
    async def handler(
        body: dict[str, int],
        value: str = Depends(injected_dependency),
    ) -> None:
        seen.append((body, value))

    async with TestServiceBusBroker(broker):
        await broker.publish({"value": 1}, queue="input")

    assert seen == [({"value": 1}, "dependency-value")]
    assert calls == ["broker", "subscriber", "injected"]


@pytest.mark.asyncio()
async def test_broker_middleware_wraps_publish_and_consume() -> None:
    events: list[str] = []

    class TrackingMiddleware(BaseMiddleware[Any, Any]):
        async def on_receive(self) -> None:
            events.append("receive")

        async def consume_scope(
            self,
            call_next: Callable[[Any], Awaitable[Any]],
            msg: Any,
        ) -> Any:
            events.append("consume-before")
            result = await call_next(msg)
            events.append("consume-after")
            return result

        async def publish_scope(
            self,
            call_next: Callable[[Any], Awaitable[Any]],
            cmd: Any,
        ) -> Any:
            events.append("publish-before")
            result = await call_next(cmd)
            events.append("publish-after")
            return result

    broker = ServiceBusBroker(
        "Endpoint=sb://unused",
        middlewares=(TrackingMiddleware,),
    )

    @broker.subscriber(queue="input")
    async def handler() -> None:
        events.append("handler")

    async with TestServiceBusBroker(broker):
        await broker.publish({}, queue="input")

    assert events == [
        "publish-before",
        "receive",
        "consume-before",
        "handler",
        "consume-after",
        "publish-after",
    ]


@pytest.mark.asyncio()
async def test_custom_parser_and_decoder_compose_with_defaults() -> None:
    parser_calls = 0
    decoder_calls = 0

    async def parser(raw: Any, original: Callable[[Any], Awaitable[Any]]) -> Any:
        nonlocal parser_calls
        parser_calls += 1
        message = await original(raw)
        message.headers["parsed"] = "yes"
        return message

    async def decoder(
        message: Any,
        original: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        nonlocal decoder_calls
        decoder_calls += 1
        body = await original(message)
        return {**body, "decoded": True}

    broker = ServiceBusBroker(
        "Endpoint=sb://unused",
        parser=parser,
        decoder=decoder,
    )
    seen: list[tuple[dict[str, Any], str]] = []

    @broker.subscriber(queue="input")
    async def handler(body: dict[str, Any], message: ServiceBusMessage) -> None:
        seen.append((body, message.headers["parsed"]))

    async with TestServiceBusBroker(broker):
        await broker.publish({"value": 1}, queue="input")

    assert seen == [({"value": 1, "decoded": True}, "yes")]
    assert parser_calls >= 1
    assert decoder_calls >= 1


class PrefixCodec:
    async def encode(
        self,
        message: Any,
        serializer: Any = None,
    ) -> tuple[bytes, str]:
        return f"encoded:{message}".encode(), "application/x-prefix"

    async def decode(self, message: Any) -> str:
        body = b"".join(message.raw_message.body).decode()
        return body.removeprefix("encoded:")


@pytest.mark.asyncio()
async def test_custom_codec_round_trips_through_test_broker() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused", codec=PrefixCodec())
    seen: list[str] = []

    @broker.subscriber(queue="input")
    async def handler(body: str) -> None:
        seen.append(body)

    async with TestServiceBusBroker(broker):
        await broker.publish("hello", queue="input")

    assert seen == ["hello"]


@pytest.mark.asyncio()
async def test_service_bus_response_metadata_reaches_publisher() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused")
    publisher = broker.publisher(queue="output")
    seen: list[ServiceBusMessage] = []

    @broker.subscriber(queue="input")
    @publisher
    async def handle() -> ServiceBusResponse:
        return ServiceBusResponse(
            {"answer": 42},
            headers={"source": "handler"},
            correlation_id="response-correlation",
            subject="answer.created",
        )

    @broker.subscriber(queue="output")
    async def capture(message: ServiceBusMessage) -> None:
        seen.append(message)

    async with TestServiceBusBroker(broker):
        await broker.publish({}, queue="input")

    assert len(seen) == 1
    assert await seen[0].decode() == {"answer": 42}
    assert seen[0].headers["source"] == "handler"
    assert seen[0].correlation_id == "response-correlation"
    assert seen[0].raw_message.subject == "answer.created"
