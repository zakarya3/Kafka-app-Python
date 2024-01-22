from confluent_kafka import Consumer, KafkaError

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Create a Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'example_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to a topic
topic = 'test'
consumer.subscribe([topic])

# Poll for messages
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print("Reached end of partition")
            else:
                print(f"Error: {msg.error()}")
        else:
            # Process the received message
            print(f"Received message: Key={msg.key()}, Value={msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    pass

finally:
    # Close the consumer
    consumer.close()
