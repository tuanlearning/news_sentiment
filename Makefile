# Spin up docker file
docker-compose up -d

## After spinning up the service you can see the dashboard at localhost:8501

# Run the producer
docker-compose run --rm python python producer/producer.py

# Run the composer
docker-compose run --rm python python consumer/consumer.py