"""Validation for biometric data to ensure realistic values."""

import logging
from typing import Optional, Union

logger = logging.getLogger(__name__)


class BiometricValidator:
    """Validates biometric data to ensure values are within realistic bounds."""

    # Realistic bounds for biometric data
    HEART_RATE_MIN = 30  # Bradycardia threshold
    HEART_RATE_MAX = 220  # Maximum theoretical heart rate
    HEART_RATE_RESTING_MIN = 40
    HEART_RATE_RESTING_MAX = 100
    HEART_RATE_ACTIVE_MAX = 200

    SKIN_TEMPERATURE_MIN_C = 20.0  # Hypothermia threshold
    SKIN_TEMPERATURE_MAX_C = 42.0  # Hyperthermia threshold
    SKIN_TEMPERATURE_NORMAL_MIN_C = 28.0  # Normal skin temp range
    SKIN_TEMPERATURE_NORMAL_MAX_C = 37.0

    SKIN_CONDUCTIVITY_MIN = 0.0  # Minimum GSR value
    SKIN_CONDUCTIVITY_MAX = 100.0  # Maximum reasonable GSR value

    ACCELEROMETER_MIN = -20.0  # -20g acceleration
    ACCELEROMETER_MAX = 20.0   # +20g acceleration

    @staticmethod
    def validate_heart_rate(value: Union[int, float], strict: bool = False) -> Optional[float]:
        """
        Validate heart rate value.

        Args:
            value: Heart rate in beats per minute
            strict: If True, use stricter validation for normal activity range

        Returns:
            Validated heart rate as float, or None if invalid
        """
        try:
            hr = float(value)

            # Check absolute bounds
            if hr < BiometricValidator.HEART_RATE_MIN or hr > BiometricValidator.HEART_RATE_MAX:
                logger.warning(f"Heart rate {hr} is outside valid range ({BiometricValidator.HEART_RATE_MIN}-{BiometricValidator.HEART_RATE_MAX} bpm)")
                return None

            # Strict validation for normal activity
            if strict:
                if hr < BiometricValidator.HEART_RATE_RESTING_MIN:
                    logger.info(f"Heart rate {hr} is below normal resting range")
                elif hr > BiometricValidator.HEART_RATE_ACTIVE_MAX:
                    logger.info(f"Heart rate {hr} is above normal active range")

            return hr

        except (ValueError, TypeError) as e:
            logger.error(f"Invalid heart rate value: {value} - {e}")
            return None

    @staticmethod
    def validate_skin_temperature(value: Union[int, float], unit: str = 'C') -> Optional[float]:
        """
        Validate skin temperature value.

        Args:
            value: Skin temperature
            unit: Temperature unit ('C' for Celsius, 'F' for Fahrenheit)

        Returns:
            Validated temperature in Celsius as float, or None if invalid
        """
        try:
            temp = float(value)

            # Convert Fahrenheit to Celsius if needed
            if unit == 'F':
                temp = (temp - 32) * 5/9

            # Check absolute bounds
            if temp < BiometricValidator.SKIN_TEMPERATURE_MIN_C or temp > BiometricValidator.SKIN_TEMPERATURE_MAX_C:
                logger.warning(f"Skin temperature {temp}°C is outside valid range ({BiometricValidator.SKIN_TEMPERATURE_MIN_C}-{BiometricValidator.SKIN_TEMPERATURE_MAX_C}°C)")
                return None

            # Log if outside normal range
            if temp < BiometricValidator.SKIN_TEMPERATURE_NORMAL_MIN_C:
                logger.info(f"Skin temperature {temp}°C is below normal range")
            elif temp > BiometricValidator.SKIN_TEMPERATURE_NORMAL_MAX_C:
                logger.info(f"Skin temperature {temp}°C is above normal range")

            return temp

        except (ValueError, TypeError) as e:
            logger.error(f"Invalid skin temperature value: {value} - {e}")
            return None

    @staticmethod
    def validate_skin_conductivity(value: Union[int, float]) -> Optional[float]:
        """
        Validate skin conductivity (GSR) value.

        Args:
            value: Skin conductivity/GSR value

        Returns:
            Validated skin conductivity as float, or None if invalid
        """
        try:
            gsr = float(value)

            # Check bounds
            if gsr < BiometricValidator.SKIN_CONDUCTIVITY_MIN or gsr > BiometricValidator.SKIN_CONDUCTIVITY_MAX:
                logger.warning(f"Skin conductivity {gsr} is outside valid range ({BiometricValidator.SKIN_CONDUCTIVITY_MIN}-{BiometricValidator.SKIN_CONDUCTIVITY_MAX})")
                return None

            return gsr

        except (ValueError, TypeError) as e:
            logger.error(f"Invalid skin conductivity value: {value} - {e}")
            return None

    @staticmethod
    def validate_accelerometer(value: Union[int, float]) -> Optional[float]:
        """
        Validate accelerometer value.

        Args:
            value: Accelerometer value in g-force

        Returns:
            Validated accelerometer value as float, or None if invalid
        """
        try:
            accel = float(value)

            # Check bounds
            if accel < BiometricValidator.ACCELEROMETER_MIN or accel > BiometricValidator.ACCELEROMETER_MAX:
                logger.warning(f"Accelerometer value {accel}g is outside valid range ({BiometricValidator.ACCELEROMETER_MIN}-{BiometricValidator.ACCELEROMETER_MAX}g)")
                return None

            return accel

        except (ValueError, TypeError) as e:
            logger.error(f"Invalid accelerometer value: {value} - {e}")
            return None

    @staticmethod
    def sanitize_biometric_data(data: dict) -> dict:
        """
        Sanitize a dictionary of biometric data, removing invalid values.

        Args:
            data: Dictionary containing biometric data

        Returns:
            Sanitized dictionary with invalid values removed or corrected
        """
        sanitized = {}

        if 'heart_rate' in data:
            validated = BiometricValidator.validate_heart_rate(data['heart_rate'])
            if validated is not None:
                sanitized['heart_rate'] = validated
            else:
                logger.warning(f"Removing invalid heart_rate: {data['heart_rate']}")

        if 'skin_temperature' in data:
            validated = BiometricValidator.validate_skin_temperature(data['skin_temperature'])
            if validated is not None:
                sanitized['skin_temperature'] = validated
            else:
                logger.warning(f"Removing invalid skin_temperature: {data['skin_temperature']}")

        if 'skin_conductivity' in data:
            validated = BiometricValidator.validate_skin_conductivity(data['skin_conductivity'])
            if validated is not None:
                sanitized['skin_conductivity'] = validated
            else:
                logger.warning(f"Removing invalid skin_conductivity: {data['skin_conductivity']}")

        return sanitized

