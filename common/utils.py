from confluent_kafka import Producer, Consumer
import logging
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def read_config():
    # reads the client configuration from client.properties
    # and returns it as a key-value map
    try:
        config = {}
        with open("credentials/client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
        return config
    except FileNotFoundError as e:
        logging.error(f"Config file error: {e}")
        raise  # Let it crash early if config is missing

def init_producer(config):
    if config is None:
        logging.error('Cannot parse config for Confluent Kafka Producer')
        return None
    try:
        return Producer(config)
    except Exception as e:
        logging.error(f'Failed to initiate Kafka producer: {e}', exc_info=True)
        return None


def init_consumer(config, group_id, offset):
    if config is None:
        logging.error('Cannot parse config for Confluent Kafka Consumer')
        return None
    try:
        config["group.id"] = group_id
        config["auto.offset.reset"] = offset
        return Consumer(config)
    except Exception as e:
        logging.error(f'Failed to initiate Kafka consumer: {e}', exc_info=True)
        return None

# Parse error used for debugging
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def produce(topic,producer,key,value,timestamp):
    producer.produce(topic, key = key, value = value, timestamp = timestamp, callback = delivery_report)


