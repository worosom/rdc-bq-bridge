"""Data decoding utilities for Redis data."""

import json
import logging
from typing import Any

import msgpack

logger = logging.getLogger(__name__)


class RedisDataDecoder:
    """Handles decoding of various data formats from Redis."""

    @staticmethod
    def decode_binary_data(data: bytes) -> Any:
        """
        Attempt to decode binary data using various methods.

        Args:
            data: Binary data to decode

        Returns:
            Decoded data or original bytes if decoding fails
        """
        if not isinstance(data, bytes):
            return data

        # Try msgpack first (common for binary Redis data)
        try:
            return msgpack.unpackb(data, raw=False, strict_map_key=False)
        except (msgpack.exceptions.ExtraData, msgpack.exceptions.UnpackException, ValueError):
            pass

        # Try UTF-8 decoding
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            pass

        # Try JSON (sometimes stored as bytes)
        try:
            text = data.decode("utf-8", errors="ignore")
            return json.loads(text)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        # Try latin-1 (accepts all byte values)
        try:
            return data.decode("latin-1")
        except Exception:
            pass

        # Return as hex string if all else fails
        logger.warning(
            f"Could not decode binary data of length {len(data)}, returning as hex string")
        return data.hex()

    @staticmethod
    def decode_hash_value(value: dict) -> dict:
        """
        Decode a Redis hash value, handling binary keys and values.

        Args:
            value: Hash dictionary from Redis

        Returns:
            Decoded hash dictionary
        """
        decoded_hash = {}
        decoder = RedisDataDecoder()

        for k, v in value.items():
            # Decode key
            if isinstance(k, bytes):
                k = decoder.decode_binary_data(k)
            # Decode value
            if isinstance(v, bytes):
                v = decoder.decode_binary_data(v)
            decoded_hash[k] = v

        return decoded_hash

    @staticmethod
    def parse_json_if_needed(value: Any, should_parse: bool) -> Any:
        """
        Parse JSON string if configured.

        Args:
            value: Value to potentially parse
            should_parse: Whether to attempt JSON parsing

        Returns:
            Parsed JSON or original value
        """
        if should_parse and isinstance(value, str):
            return json.loads(value)
        return value

