from confluent_kafka import Consumer
import json
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.utils import read_config, init_consumer
from google.cloud import bigquery
from dotenv import load_dotenv

def consume(consumer, topic, bq_client, PROJECT_ID, DATASET_ID, TABLE_ID):
    # subscribes to the specified topic
    consumer.subscribe([topic])

    try:
        while True:
            # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                key = msg.key().decode("utf-8") if msg.key() is not None else None
                value = msg.value().decode("utf-8")
                value_dict = json.loads(value)
                value_dict['published_date'] = value_dict['published_date'].replace('Z','').replace('T',' ')
                if exist_same_url(bq_client, PROJECT_ID, DATASET_ID, TABLE_ID, value_dict['url']):
                    logging.info(f'Skip this record from topic {topic}: key = {key:12} value = {value:12} because already exist record with same url')
                    continue
                else:
                    bq_client.insert_rows_json(f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}', [value_dict])
                logging.info(f"Consumed message from topic {topic}: key = {key:12} value = {value:12}")
    except Exception as e:
        logging.error(f'Cannot consume the record: {e}')
        logging.error(f'{value}')
    except KeyboardInterrupt:
        consumer.close()
        logging.info('User close this consumer')

def exist_same_url(bq_client, PROJECT_ID, DATASET_ID, TABLE_ID, url):
    try:
        query = f"""
        SELECT COUNT(1) as url_count 
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE url = @url
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("url", "STRING", url)]
        )
        result = bq_client.query(query, job_config=job_config).result()
        row = list(result)[0]
        return row.url_count > 0
    except Exception as e:
        logging.error(f"Failed to check URL existence for {url}: {e}")
        return False  # Fail-safe: assume it doesn’t exist

def create_table(bq_client, PROJECT_ID, DATASET_ID, TABLE_ID):
    try:
        query = f"""
            CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            (
            source_id STRING,
            source_name STRING,
            author STRING,
            title STRING,
            sentiment_polarity FLOAT64,
            sentiment STRING,
            description STRING,
            url STRING,
            published_date DATETIME
            )
            CLUSTER BY url, published_date;
        """
        bq_client.query(query)
        logging.info('Successfully create table')
    except Exception as e:
        logging.error(f"Cannot create table: {e}")

def main():
    TOPIC = 'apple_news'
    config = read_config()
    consumer = init_consumer(config, 'python-group-1', 'earliest')
    # Bigquery sink table
    PROJECT_ID = "august-sandbox-425102-m1"
    DATASET_ID = "news"
    TABLE_ID = "apple_news"
    load_dotenv()
    bq_client = bigquery.Client()
    create_table(bq_client, PROJECT_ID, DATASET_ID, TABLE_ID)

    if not consumer:
        logging.error('Cannot create the consumer')
        raise

    consume(consumer, TOPIC, bq_client, PROJECT_ID, DATASET_ID, TABLE_ID)
    
if __name__ == '__main__':
    main()