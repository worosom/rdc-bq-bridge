"""Redis data event model for the RDC-BigQuery bridge."""

from typing import Any, Optional

from .config import RoutingRule


class RedisDataEvent:
    """Represents a data event from Redis."""

    def __init__(
        self,
        source_type: str,  # "key" or "channel"
        key_or_channel: str,
        value: Any,
        routing_rule: Optional[RoutingRule] = None,
        ttl: Optional[int] = None  # TTL in seconds: None = no expiry, positive = TTL
    ):
        self.source_type = source_type
        self.key_or_channel = key_or_channel
        self.value = value
        self.routing_rule = routing_rule
        self.ttl = ttl