from dataclasses import fields

from faststream.specification.schema import SubscriberSpec

# FastStream main made `address` a required field on SubscriberSpec and
# PublisherSpec. Released 0.7.x does not have it yet, so only pass it when
# the installed version knows about it.
_SPEC_HAS_ADDRESS = "address" in {f.name for f in fields(SubscriberSpec)}


def spec_address(address: str) -> dict[str, str]:
    return {"address": address} if _SPEC_HAS_ADDRESS else {}
