# News Sentiment Analysis Pipeline

This project implements a full pipeline for acquiring, processing, and visualizing news data with sentiment analysis. It consists of three main components:

1. **Kafka Producer**: Fetches news data from NewsAPI and performs sentiment analysis.
2. **Kafka Consumer**: Reads the processed news data and loads it into Google BigQuery.
3. **Streamlit Dashboard**: Displays the analyzed data in an interactive dashboard.

## Tech Stack

- **Apache Kafka** - Message broker for real-time data streaming.
- **Python** - Used for the producer, consumer, and dashboard.
- **Google BigQuery** - Cloud data warehouse for storing processed news data.
- **Streamlit** - Web framework for data visualization.
- **Docker & Docker Compose** - For containerized deployment.

---

## 🚀 Getting Started

### Prerequisites

1. Install [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/).
2. Get a NewsAPI key from [NewsAPI](https://newsapi.org/) and set it in the environment.
3. Create a Confluent account and create a topic name applle_news[Confluent](https://www.confluent.io/), after that get a client.properties and place it to credentials folder
4. Get a service account for your Google Cloud Platform with Google Big Query Editor role and place it into credentials folder with the name gcp.json

### Setup & Running

Spin up the required services:

```sh
docker-compose up -d
```

Once the service is running, you can access the Streamlit dashboard at:  
📍 **[http://localhost:8501](http://localhost:8501)**

### Running Producer & Consumer

Run the Kafka producer to fetch news data:

```sh
docker-compose run --rm python python producer/producer.py
```

Run the Kafka consumer to store processed data in BigQuery:

```sh
docker-compose run --rm python python consumer/consumer.py
```

---

## 📊 Streamlit Dashboard

- Displays sentiment trends over time.
- Allows users to explore individual news articles.

To start the dashboard manually:

```sh
docker-compose run --rm python streamlit run dashboard/apple_news_dashboard.py
```

---

## 🛠 Development

To install dependencies inside the Docker container:

```sh
docker-compose run --rm python pip install -r requirements.txt
```

If you update `requirements.txt`, rebuild the container:

```sh
docker-compose up -d --build
```

---

## 📄 License

This project is for educational purposes only.

---
🔥 Built with passion by **Flink Bro & Co.** 🚀
