from confluent_kafka import Consumer
import json
import logging
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.utils import read_config, init_consumer

def consume(consumer, topic):
    # subscribes to the specified topic
    consumer.subscribe([topic])

    try:
        while True:
            # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                key = msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                print(f"Consumed message from topic {topic}: key = {key:12} value = {value:12}")
    except Exception as e:
        logging.error(f'Cannot consume the record: {e}')
    except KeyboardInterrupt:
        consumer.close()
        logging.info('User close this consumer')

def main():
    TOPIC = 'apple_news'
    config = read_config()
    consumer = init_consumer(config, 'python-group-1', 'earliest')

    if not consumer:
        logging.error('Cannot create the consumer')
        raise

    consume(consumer, TOPIC)
    
if __name__ == '__main__':
    main()