"""Routing rule management for Redis data."""

import fnmatch
import logging
from typing import List, Optional

from .config import Config, RoutingRule

logger = logging.getLogger(__name__)


class RoutingManager:
    """Manages routing rules for Redis keys and channels."""

    def __init__(self, config: Config):
        self.config = config

        # Separate routing rules by source type
        all_key_rules = [
            rule for rule in config.pipeline.routing_rules
            if rule.redis_source_type == "key"
        ]
        all_channel_rules = [
            rule for rule in config.pipeline.routing_rules
            if rule.redis_source_type == "channel"
        ]

        # Sort rules: specific patterns first, catch-all patterns last
        self.key_rules = self._sort_rules_by_specificity(all_key_rules)
        self.channel_rules = self._sort_rules_by_specificity(all_channel_rules)

        logger.info(
            f"Initialized RoutingManager with {len(self.key_rules)} key rules "
            f"and {len(self.channel_rules)} channel rules"
        )

        # Log the rule order for debugging
        self._log_rule_order()

    def _sort_rules_by_specificity(self, rules: List[RoutingRule]) -> List[RoutingRule]:
        """
        Sort rules by specificity - more specific patterns first, catch-all patterns last.

        Rules are sorted by:
        1. Rules without wildcards come first
        2. Rules with specific prefixes before wildcards come next
        3. Rules with only wildcards (like '*') come last
        """
        def get_specificity_score(rule: RoutingRule) -> tuple:
            pattern = rule.redis_pattern

            # Pattern with no wildcards gets highest priority (score 0)
            if '*' not in pattern and '?' not in pattern:
                return (0, -len(pattern))

            # Pattern that starts with specific text before wildcard gets medium priority
            if '*' in pattern:
                prefix_before_wildcard = pattern.split('*')[0]
                if prefix_before_wildcard:
                    return (1, -len(prefix_before_wildcard))

            # Pure wildcard pattern gets lowest priority
            if pattern == '*':
                return (2, 0)

            # Other wildcard patterns get medium-low priority
            return (1.5, -len(pattern))

        return sorted(rules, key=get_specificity_score)

    def _log_rule_order(self):
        """Log the order of routing rules for debugging."""
        if self.key_rules:
            logger.info("Key rules order (most specific first):")
            for i, rule in enumerate(self.key_rules, 1):
                logger.info(
                    f"  {i}. {rule.name}: pattern='{rule.redis_pattern}' "
                    f"-> table={rule.target_table}"
                )

        if self.channel_rules:
            logger.info("Channel rules order (most specific first):")
            for i, rule in enumerate(self.channel_rules, 1):
                logger.info(
                    f"  {i}. {rule.name}: pattern='{rule.redis_pattern}' "
                    f"-> table={rule.target_table}"
                )

    def find_matching_key_rule(self, key: str) -> Optional[RoutingRule]:
        """Find a routing rule that matches the given key."""
        for rule in self.key_rules:
            if self._matches_pattern(key, rule.redis_pattern):
                logger.debug(
                    f"Key '{key}' matches pattern '{rule.redis_pattern}' "
                    f"from rule '{rule.name}'"
                )
                return rule
        logger.debug(f"Key '{key}' did not match any rule patterns")
        return None

    def find_matching_channel_rule(self, channel: str) -> Optional[RoutingRule]:
        """Find a routing rule that matches the given channel."""
        for rule in self.channel_rules:
            if self._matches_pattern(channel, rule.redis_pattern):
                logger.debug(
                    f"Channel '{channel}' matches pattern '{rule.redis_pattern}' "
                    f"from rule '{rule.name}'"
                )
                return rule
        logger.debug(f"Channel '{channel}' did not match any rule patterns")
        return None

    @staticmethod
    def _matches_pattern(text: str, pattern: str) -> bool:
        """Check if text matches the pattern (supports * wildcards)."""
        return fnmatch.fnmatch(text, pattern)

    def has_key_rules(self) -> bool:
        """Check if there are any key-based routing rules."""
        return bool(self.key_rules)

    def has_channel_rules(self) -> bool:
        """Check if there are any channel-based routing rules."""
        return bool(self.channel_rules)

    def get_channel_patterns(self) -> List[str]:
        """Get all unique channel patterns for subscription."""
        return [rule.redis_pattern for rule in self.channel_rules]
    
    def get_key_prefixes(self) -> List[str]:
        """
        Extract unique prefixes from key routing rules for CLIENT TRACKING.
        
        Converts patterns like:
          - "Visitors:*:Status" → "Visitors:"
          - "Wearables:Scent:*" → "Wearables:"
          - "IO:*" → "IO:"
        
        Returns:
            List of unique prefix strings (e.g., ["Visitors:", "Wearables:", "IO:"])
        """
        prefixes = set()
        
        for rule in self.key_rules:
            pattern = rule.redis_pattern
            
            # Extract prefix before first wildcard
            if '*' in pattern:
                prefix = pattern.split('*')[0]
                # Ensure prefix ends with delimiter (typically ':')
                if prefix and not prefix.endswith(':'):
                    # Find the last colon
                    last_colon = prefix.rfind(':')
                    if last_colon != -1:
                        prefix = prefix[:last_colon + 1]
                
                if prefix:  # Only add non-empty prefixes
                    prefixes.add(prefix)
            else:
                # Exact match pattern - use entire string as prefix
                prefixes.add(pattern)
        
        sorted_prefixes = sorted(prefixes)
        logger.info(f"Extracted {len(sorted_prefixes)} unique prefixes for CLIENT TRACKING")
        
        return sorted_prefixes

    def get_key_patterns(self) -> List[str]:
        """Get all unique key patterns for client tracking."""
        return [rule.redis_pattern for rule in self.key_rules]