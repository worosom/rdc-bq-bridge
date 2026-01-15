
import redis
import msgpack
import logging
from src.config import load_config, get_default_config_path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """
    Subscribes to a Redis channel, decodes messages using MessagePack, and logs them.
    """
    try:
        # Load configuration
        config_path = get_default_config_path()
        config = load_config(config_path)
        logging.info("Configuration loaded successfully.")

        # Get Redis connection details from config
        redis_host = config.redis.host
        redis_port = config.redis.port
        redis_username = config.redis.username
        redis_password = config.redis.password

        # Prompt user for the channel name
        channel_name = input("Enter the Redis channel to subscribe to: ")

        # Connect to Redis
        r = redis.Redis(
            host=redis_host,
            port=redis_port,
            username=redis_username,
            password=redis_password,
            decode_responses=False  # We want bytes for msgpack
        )
        r.ping()
        logging.info(f"Connected to Redis at {redis_host}:{redis_port}")

        # Subscribe to the channel
        pubsub = r.pubsub()
        pubsub.subscribe(channel_name)
        logging.info(f"Subscribed to channel: {channel_name}")

        # Listen for messages
        for message in pubsub.listen():
            if message['type'] == 'message':
                try:
                    # The data is in binary format, decode it with msgpack
                    decoded_data = msgpack.unpackb(message['data'], raw=False)
                    logging.info(f"Received message: {decoded_data}")
                except Exception as e:
                    logging.error(f"Could not decode message: {e}")
                    logging.info(f"Raw message data: {message['data']}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
