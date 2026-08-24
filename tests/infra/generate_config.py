"""Generate the Service Bus emulator's Config.json.

The emulator cannot create entities on the fly the way the test suite's
`uuid4()` queue names would need, and it caps a namespace at 50 entities. So the
test suite leases names from a fixed pool instead, and that pool has to be
declared before the container starts.

Run after changing the pool sizes:

    python tests/infra/generate_config.py
"""

import json
from pathlib import Path

NAMESPACE = "sbemulatorns"

QUEUE_COUNT = 24
TOPIC_COUNT = 6
SUBSCRIPTIONS_PER_TOPIC = 2
EXAMPLE_QUEUES = (
    "example-queue",
    "example-messagepack",
    "example-requests",
    "example-replies",
)

# Deliberately short so lock-expiry behaviour is testable without slow tests.
# PT5S is the service minimum.
LOCK_DURATION = "PT30S"
SHORT_LOCK_DURATION = "PT5S"

# Queues reserved for tests that need a lock to expire mid-handler.
SHORT_LOCK_QUEUE_COUNT = 4

QUEUE_PROPERTIES = {
    "DeadLetteringOnMessageExpiration": False,
    "DefaultMessageTimeToLive": "PT1H",
    "DuplicateDetectionHistoryTimeWindow": "PT20S",
    "ForwardDeadLetteredMessagesTo": "",
    "ForwardTo": "",
    "LockDuration": LOCK_DURATION,
    "MaxDeliveryCount": 10,
    "RequiresDuplicateDetection": False,
    "RequiresSession": False,
}

TOPIC_PROPERTIES = {
    "DefaultMessageTimeToLive": "PT1H",
    "DuplicateDetectionHistoryTimeWindow": "PT20S",
    "RequiresDuplicateDetection": False,
}

SUBSCRIPTION_PROPERTIES = {
    "DeadLetteringOnMessageExpiration": False,
    "DefaultMessageTimeToLive": "PT1H",
    "LockDuration": LOCK_DURATION,
    "MaxDeliveryCount": 10,
    "ForwardDeadLetteredMessagesTo": "",
    "ForwardTo": "",
    "RequiresSession": False,
}


def build_config() -> dict[str, object]:
    queues = [
        {"Name": name, "Properties": dict(QUEUE_PROPERTIES)} for name in EXAMPLE_QUEUES
    ]
    for index in range(QUEUE_COUNT):
        properties = dict(QUEUE_PROPERTIES)
        if index < SHORT_LOCK_QUEUE_COUNT:
            properties["LockDuration"] = SHORT_LOCK_DURATION
        queues.append({"Name": f"test-queue-{index:02d}", "Properties": properties})

    topics = [
        {
            "Name": f"test-topic-{index:02d}",
            "Properties": dict(TOPIC_PROPERTIES),
            "Subscriptions": [
                {
                    "Name": f"sub-{sub_index}",
                    "Properties": dict(SUBSCRIPTION_PROPERTIES),
                    "Rules": [],
                }
                for sub_index in range(SUBSCRIPTIONS_PER_TOPIC)
            ],
        }
        for index in range(TOPIC_COUNT)
    ]

    entity_count = len(queues) + len(topics)
    if entity_count > 50:
        msg = f"emulator allows 50 entities per namespace, got {entity_count}"
        raise ValueError(msg)

    return {
        "UserConfig": {
            "Namespaces": [
                {"Name": NAMESPACE, "Queues": queues, "Topics": topics},
            ],
            "Logging": {"Type": "File"},
        },
    }


if __name__ == "__main__":
    target = Path(__file__).parent / "servicebus-config.json"
    target.write_text(json.dumps(build_config(), indent=2) + "\n")
    print(f"wrote {target}")
