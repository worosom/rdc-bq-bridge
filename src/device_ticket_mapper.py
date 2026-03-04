"""Device-to-ticket mapping for enriching biometric data with visitor ticket_id."""

import asyncio
import logging
import msgpack
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class DeviceTicketMapper:
    """
    Maintains bidirectional mappings between device IDs and ticket IDs.
    
    This enables automatic enrichment of biometric data (empatica, blueiot)
    with visitor ticket_id by tracking which devices are currently assigned
    to which visitors.
    
    The mapper monitors Redis keys of the form:
        Visitors:{ticket_id}:EmpaticaDeviceID
    
    Where the value is a msgpack-encoded device_id string.
    
    Usage:
        mapper = DeviceTicketMapper()
        
        # Bootstrap from existing Redis data
        await mapper.bootstrap_from_redis(redis_client)
        
        # Update mappings as events arrive
        await mapper.update_mapping(device_id="E332003716C9", ticket_id="TICKET123")
        
        # Enrich data by looking up ticket_id from device_id
        ticket_id = await mapper.get_ticket_for_device("E332003716C9")
        
        # Release mapping when visitor becomes inactive
        await mapper.remove_mapping(device_id="E332003716C9")
    """

    def __init__(self):
        """Initialize the mapper with empty bidirectional dictionaries."""
        # Bidirectional mappings
        self._device_to_ticket: Dict[str, str] = {}
        self._ticket_to_device: Dict[str, str] = {}
        
        # Lock for thread-safe updates
        self._lock = asyncio.Lock()
        
        # Statistics
        self._total_updates = 0
        self._total_removals = 0
        self._total_lookups = 0
        
        logger.info("DeviceTicketMapper initialized")

    async def bootstrap_from_redis(self, redis_client) -> int:
        """
        Bootstrap device-to-ticket mappings from existing Redis keys.
        
        Scans for all Visitors:*:EmpaticaDeviceID keys and populates
        the mapping dictionaries.
        
        Args:
            redis_client: Redis client instance
            
        Returns:
            Number of mappings loaded
            
        Note:
            This should be called once at startup before processing events.
        """
        count = 0
        
        try:
            logger.info("Scanning Redis for existing device-ticket mappings...")
            
            # Scan for all Visitors:*:EmpaticaDeviceID keys
            pattern = "Visitors:*:EmpaticaDeviceID"
            cursor = 0
            
            while True:
                cursor, keys = await redis_client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=100
                )
                
                for key in keys:
                    try:
                        # Get the device_id value (msgpack-encoded)
                        device_id_raw = await redis_client.get(key)
                        
                        if device_id_raw is None:
                            continue
                        
                        # Decode msgpack
                        try:
                            if isinstance(device_id_raw, bytes):
                                device_id = msgpack.unpackb(device_id_raw, raw=False)
                            else:
                                device_id = device_id_raw
                        except (msgpack.exceptions.ExtraData, msgpack.exceptions.UnpackException):
                            # Fallback: try as plain string
                            if isinstance(device_id_raw, bytes):
                                device_id = device_id_raw.decode('utf-8', errors='ignore')
                            else:
                                device_id = str(device_id_raw)
                        
                        # Normalize device_id
                        device_id = str(device_id).strip()
                        
                        # Skip empty device IDs
                        if not device_id:
                            continue
                        
                        # Extract ticket_id from key: "Visitors:TICKET123:EmpaticaDeviceID"
                        if isinstance(key, bytes):
                            key = key.decode('utf-8')
                        
                        parts = key.split(":")
                        if len(parts) >= 3 and parts[0] == "Visitors":
                            ticket_id = parts[1].strip()
                            
                            # Add to mappings
                            async with self._lock:
                                # Check if device is already mapped to a different ticket
                                existing_ticket = self._device_to_ticket.get(device_id)
                                if existing_ticket and existing_ticket != ticket_id:
                                    logger.warning(
                                        f"Bootstrap: Device {device_id} was mapped to "
                                        f"{existing_ticket}, now remapping to {ticket_id}"
                                    )
                                    # Remove old ticket → device mapping
                                    if existing_ticket in self._ticket_to_device:
                                        del self._ticket_to_device[existing_ticket]
                                
                                # Check if ticket already has a different device
                                existing_device = self._ticket_to_device.get(ticket_id)
                                if existing_device and existing_device != device_id:
                                    logger.warning(
                                        f"Bootstrap: Ticket {ticket_id} had device "
                                        f"{existing_device}, now assigning {device_id}"
                                    )
                                    # Remove old device → ticket mapping
                                    if existing_device in self._device_to_ticket:
                                        del self._device_to_ticket[existing_device]
                                
                                # Set bidirectional mapping
                                self._device_to_ticket[device_id] = ticket_id
                                self._ticket_to_device[ticket_id] = device_id
                                count += 1
                            
                            logger.debug(f"Bootstrap: {device_id} → {ticket_id}")
                    
                    except Exception as e:
                        logger.error(f"Error processing key {key} during bootstrap: {e}")
                        continue
                
                # Break if we've processed all keys
                if cursor == 0:
                    break
            
            logger.info(f"Bootstrap complete: loaded {count} device-ticket mappings")
            
        except Exception as e:
            logger.error(f"Error during bootstrap from Redis: {e}", exc_info=True)
        
        return count

    async def update_mapping(self, device_id: str, ticket_id: str) -> None:
        """
        Update or create a device-to-ticket mapping.
        
        This handles several scenarios:
        1. New assignment: device and ticket are both new
        2. Device reassignment: device moves from one ticket to another
        3. Ticket reassignment: ticket gets a new device (old device is released)
        
        Args:
            device_id: The device identifier (e.g., "E332003716C9")
            ticket_id: The visitor ticket identifier (e.g., "TICKET123")
        """
        async with self._lock:
            # Check if device is already mapped to a different ticket
            existing_ticket = self._device_to_ticket.get(device_id)
            if existing_ticket and existing_ticket != ticket_id:
                logger.info(
                    f"Device {device_id} reassigned from {existing_ticket} to {ticket_id}"
                )
                # Remove old ticket → device mapping
                if existing_ticket in self._ticket_to_device:
                    del self._ticket_to_device[existing_ticket]
            
            # Check if ticket already has a different device
            existing_device = self._ticket_to_device.get(ticket_id)
            if existing_device and existing_device != device_id:
                logger.info(
                    f"Ticket {ticket_id} changed device from {existing_device} to {device_id}"
                )
                # Remove old device → ticket mapping
                if existing_device in self._device_to_ticket:
                    del self._device_to_ticket[existing_device]
            
            # Set bidirectional mapping
            self._device_to_ticket[device_id] = ticket_id
            self._ticket_to_device[ticket_id] = device_id
            self._total_updates += 1
            
            logger.debug(f"Mapping updated: {device_id} ↔ {ticket_id}")

    async def remove_mapping(self, device_id: str) -> None:
        """
        Remove a device-to-ticket mapping.
        
        This should be called when:
        - A visitor becomes inactive
        - A device is unassigned (EmpaticaDeviceID set to null/empty)
        - A device needs to be released for reassignment
        
        Args:
            device_id: The device identifier to release
        """
        async with self._lock:
            ticket_id = self._device_to_ticket.get(device_id)
            
            if ticket_id:
                # Remove bidirectional mapping
                del self._device_to_ticket[device_id]
                if ticket_id in self._ticket_to_device:
                    del self._ticket_to_device[ticket_id]
                
                self._total_removals += 1
                logger.debug(f"Mapping removed: {device_id} ↔ {ticket_id}")
            else:
                logger.debug(f"No mapping found to remove for device: {device_id}")

    async def get_ticket_for_device(self, device_id: str) -> Optional[str]:
        """
        Look up the ticket_id for a given device_id.
        
        This is used to enrich biometric data with visitor ticket_id.
        
        Args:
            device_id: The device identifier
            
        Returns:
            The ticket_id if mapped, None otherwise
        """
        self._total_lookups += 1
        ticket_id = self._device_to_ticket.get(device_id)
        
        if ticket_id:
            logger.debug(f"Lookup: {device_id} → {ticket_id}")
        
        return ticket_id

    async def get_device_for_ticket(self, ticket_id: str) -> Optional[str]:
        """
        Look up the device_id for a given ticket_id.
        
        This is used to handle status changes and device releases.
        
        Args:
            ticket_id: The visitor ticket identifier
            
        Returns:
            The device_id if mapped, None otherwise
        """
        device_id = self._ticket_to_device.get(ticket_id)
        
        if device_id:
            logger.debug(f"Reverse lookup: {ticket_id} → {device_id}")
        
        return device_id

    def get_stats(self) -> Dict[str, int]:
        """
        Get statistics about the mapper's usage.
        
        Returns:
            Dictionary with statistics:
            - active_mappings: Current number of active device-ticket pairs
            - total_updates: Total number of mapping updates
            - total_removals: Total number of mapping removals
            - total_lookups: Total number of lookup operations
        """
        return {
            "active_mappings": len(self._device_to_ticket),
            "total_updates": self._total_updates,
            "total_removals": self._total_removals,
            "total_lookups": self._total_lookups
        }

    def clear(self) -> None:
        """
        Clear all mappings.
        
        This is primarily useful for testing.
        """
        self._device_to_ticket.clear()
        self._ticket_to_device.clear()
        logger.info("All mappings cleared")
