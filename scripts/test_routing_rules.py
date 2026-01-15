#!/usr/bin/env python3
"""Test script to verify routing rule matching logic."""

import fnmatch
import re
from typing import Optional


class RoutingRule:
    """Simplified routing rule for testing."""
    def __init__(self, name: str, redis_source_type: str, redis_pattern: str,
                 target_table: str, id_parser_regex: str):
        self.name = name
        self.redis_source_type = redis_source_type
        self.redis_pattern = redis_pattern
        self.target_table = target_table
        self.id_parser_regex = id_parser_regex
        self.compiled_regex = re.compile(id_parser_regex) if id_parser_regex else None


def matches_pattern(text: str, pattern: str) -> bool:
    """Check if text matches the pattern (supports * wildcards)."""
    return fnmatch.fnmatch(text, pattern)


def find_matching_rule(key: str, rules: list) -> Optional[RoutingRule]:
    """Find the first matching rule for a key."""
    for rule in rules:
        if matches_pattern(key, rule.redis_pattern):
            return rule
    return None


def test_routing():
    """Test the routing logic with sample data."""

    # Create routing rules in the same order as config.yaml
    rules = [
        RoutingRule(
            name="VisitorBioSensors",
            redis_source_type="channel",
            redis_pattern="Wearables:WatchDevices:*:BioSensors",
            target_table="visitor_events",
            id_parser_regex=r"Wearables:WatchDevices:(?P<device_id>[^:]+):BioSensors"
        ),
        RoutingRule(
            name="VisitorPosition",
            redis_source_type="key",
            redis_pattern="Wearables:WatchDevices:*:Position",
            target_table="visitor_events",
            id_parser_regex=r"Wearables:WatchDevices:(?P<device_id>[^:]+):Position"
        ),
        RoutingRule(
            name="GlobalState",
            redis_source_type="key",
            redis_pattern="*",  # This matches EVERYTHING!
            target_table="global_state_events",
            id_parser_regex=r"(?P<state_key>.*)"
        )
    ]

    # Separate rules by type (as done in RedisIngestor)
    key_rules = [r for r in rules if r.redis_source_type == "key"]
    channel_rules = [r for r in rules if r.redis_source_type == "channel"]

    print("KEY RULES (in order):")
    for rule in key_rules:
        print(f"  - {rule.name}: pattern='{rule.redis_pattern}', target={rule.target_table}")

    print("\nCHANNEL RULES (in order):")
    for rule in channel_rules:
        print(f"  - {rule.name}: pattern='{rule.redis_pattern}', target={rule.target_table}")

    # Test cases
    test_keys = [
        "Wearables:WatchDevices:EMB001:Position",
        "Wearables:WatchDevices:EMB002:Position",
        "Rooms:MainHall:Audio:MasterVolume",
        "Installation:State:Active"
    ]

    print("\n" + "="*60)
    print("TESTING KEY MATCHING:")
    print("="*60)

    for key in test_keys:
        matching_rule = find_matching_rule(key, key_rules)
        if matching_rule:
            print(f"\nKey: {key}")
            print(f"  → Matched rule: {matching_rule.name}")
            print(f"  → Target table: {matching_rule.target_table}")
            print(f"  → Pattern: {matching_rule.redis_pattern}")
        else:
            print(f"\nKey: {key}")
            print(f"  → No matching rule found")

    # Test channels
    test_channels = [
        "Wearables:WatchDevices:EMB001:BioSensors",
        "Wearables:WatchDevices:EMB002:BioSensors"
    ]

    print("\n" + "="*60)
    print("TESTING CHANNEL MATCHING:")
    print("="*60)

    for channel in test_channels:
        matching_rule = find_matching_rule(channel, channel_rules)
        if matching_rule:
            print(f"\nChannel: {channel}")
            print(f"  → Matched rule: {matching_rule.name}")
            print(f"  → Target table: {matching_rule.target_table}")
        else:
            print(f"\nChannel: {channel}")
            print(f"  → No matching rule found")

    print("\n" + "="*60)
    print("PROBLEM IDENTIFIED:")
    print("="*60)
    print("\nThe issue is that GlobalState rule with pattern '*' matches EVERYTHING.")
    print("Since it appears before VisitorPosition in the key_rules list,")
    print("it will always match first, causing all key events to go to global_state_events table.")
    print("\nSOLUTION: Rules with wildcard patterns should be evaluated LAST.")


if __name__ == "__main__":
    test_routing()

