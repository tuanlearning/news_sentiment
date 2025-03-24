from dotenv import load_dotenv
import datetime
import os
import requests
from confluent_kafka import Producer, Consumer
import requests
import json
import logging
import time
from textblob import TextBlob
logging.basicConfig(level=logging.INFO)  # Or DEBUG

def get_from_date(n):
    from_date = (datetime.datetime.now() - datetime.timedelta(days = n)).date()
    from_date_str = datetime.datetime.strftime(from_date, "%Y-%m-%d")
    return from_date_str

def read_config():
    # reads the client configuration from client.properties
    # and returns it as a key-value map
    try:
        config = {}
        with open("client.properties") as fh:
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

# Parse error used for debugging
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def produce(topic,producer,key,value,timestamp):
    producer.produce(topic, key = key, value = value, timestamp = timestamp, callback = delivery_report)

def request(news_api, from_date):
    url = (f'https://newsapi.org/v2/everything?'
        f'q=Apple&'
        f'from={from_date}&'
        f'sortBy=popularity&'
        f'apiKey={news_api}')
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f'News API error: {response.status_code}: {response.text}')
    else:
        articles = response.json()['articles']
        logging.info(f'Successfully extract the data from: {from_date}')
        return articles

def sentiment_score(s):
    if not s:
        return None,None

    blob = TextBlob(s)
    sentiment_polarity = blob.sentiment.polarity
    if sentiment_polarity > 0:
        sentiment = 'Positive'
    elif sentiment_polarity < 0:
        sentiment = 'Negative'
    else:
        sentiment = 'Neutral'

    return (sentiment_polarity, sentiment)

def parse_article(article):
    try:
        source_id = article['source'].get('id')
        source_name = article['source'].get('name')
        author = article['author']
        title = article['title']
        sentiment_polarity, sentiment = sentiment_score(title)
        description = article['description']
        url = article['url']
        published_date = article['publishedAt']
        article_dictionary = {'source_id':source_id,\
                            'source_name':source_name,\
                            'author':author,\
                            'title':title,\
                            'sentiment_polarity':sentiment_polarity,\
                            'sentiment':sentiment,\
                            'description':description,\
                            'url':url,\
                            'published_date':published_date\
                            }
        key = json.dumps(url).encode('utf-8')
        value = json.dumps(article_dictionary).encode('utf-8')
        # Parse timestamp
        published_dt = datetime.datetime.strptime(published_date.replace('T',' ').replace('Z',''), '%Y-%m-%d %H:%M:%S')
        timestamp_millis = int(published_dt.timestamp() * 1000)
        return (key,value,timestamp_millis)
    except Exception as e:
        print(str(e))
def main():
    # Load variables from .env file
    load_dotenv()
    # Access variables
    NEWS_API = os.getenv("news_api")
    if not NEWS_API:
        logging.error("Missing NEWS_API key in .env")
        return
    TOPIC = 'apple_news'
    config = read_config()
    if config is None:
        return #Cannot proceed without kafka config
    producer = init_producer(config)
    
    while True:
        from_date = get_from_date(1)
        articles = request(NEWS_API, from_date)
        if not articles:
            logging.info('There are no new articles')
            return
        for article in articles:
            key, value, timestamp_millis = parse_article(article)
            if key and value:
                produce(TOPIC,producer,key,value,timestamp_millis)
            
        producer.flush()
        logging.info('Successfully ingest 1 batch')
        time.sleep(30 * 60)


if __name__ == '__main__':
    main()