import asyncio
import os

from faststream_azure_servicebus import ServiceBusBroker

EMULATOR_CONNECTION_STRING = (
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"  # pragma: allowlist secret
)
REQUEST_QUEUE = "example-requests"
REPLY_QUEUE = "example-replies"


async def main() -> None:
    connection_string = os.environ.get(
        "SERVICEBUS_CONNECTION_STRING",
        EMULATOR_CONNECTION_STRING,
    )
    broker = ServiceBusBroker(
        connection_string,
        reply_queue=REPLY_QUEUE,
    )

    @broker.subscriber(REQUEST_QUEUE)
    async def double(body: dict[str, int]) -> dict[str, int]:
        return {"answer": body["value"] * 2}

    try:
        await broker.start()
        response = await broker.request(
            {"value": 21},
            queue=REQUEST_QUEUE,
            timeout=30,
        )
        print(f"reply: {await response.decode()}")
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
