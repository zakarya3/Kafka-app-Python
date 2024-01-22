from confluent_kafka import Producer

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Create a Kafka producer configuration
producer_config = {
    'bootstrap.servers': bootstrap_servers,
}

# Create a Kafka producer instance
producer = Producer(producer_config)

# Produce a message to a specific topic
topic = 'test'
message = input("type message: ")

# Produce the message
producer.produce(topic, value=message)

# Wait for any outstanding messages to be delivered and delivery reports received
producer.flush()
