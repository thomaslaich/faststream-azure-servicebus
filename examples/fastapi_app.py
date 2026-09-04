import os
from typing import Annotated

from fastapi import Depends, FastAPI
from faststream_fastapi import FastStreamAPI

from faststream_azure_servicebus import ServiceBusBroker

EMULATOR_CONNECTION_STRING = (
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"  # pragma: allowlist secret
)
QUEUE = "example-queue"

broker = ServiceBusBroker(
    os.environ.get("SERVICEBUS_CONNECTION_STRING", EMULATOR_CONNECTION_STRING)
)
api = FastAPI(title="FastStream Azure Service Bus example")


def message_source() -> str:
    """A regular FastAPI dependency, shared by message handlers."""
    return "fastapi"


@broker.subscriber(QUEUE)
async def handle_order(
    body: dict[str, int],
    source: Annotated[str, Depends(message_source)],
) -> None:
    print(f"received from {source}: {body}")


@api.post("/orders/{order_id}")
async def create_order(order_id: int) -> dict[str, int]:
    await broker.publish({"id": order_id}, queue=QUEUE)
    return {"queued": order_id}


# Create the wrapper after registering every subscriber so the plugin can adapt
# their dependency injection to FastAPI.
app = FastStreamAPI(broker, application=api, asyncapi_path="/asyncapi")
