from typing import Annotated

import pytest
from fastapi import Depends, FastAPI
from faststream_fastapi import FastStreamAPI

from faststream_azure_servicebus import ServiceBusBroker, TestServiceBusBroker


@pytest.mark.asyncio()
async def test_fastapi_dependency_injection_and_overrides() -> None:
    broker = ServiceBusBroker("Endpoint=sb://unused")
    api = FastAPI()
    received: list[tuple[dict[str, int], str]] = []

    def source() -> str:
        return "original"

    api.dependency_overrides[source] = lambda: "overridden"

    @broker.subscriber(queue="orders")
    async def handler(
        body: dict[str, int],
        dependency: Annotated[str, Depends(source)],
    ) -> None:
        received.append((body, dependency))

    app = FastStreamAPI(broker, application=api)

    async with TestServiceBusBroker(broker):
        await broker.publish({"id": 1}, queue="orders")

    assert callable(app)
    assert received == [({"id": 1}, "overridden")]
