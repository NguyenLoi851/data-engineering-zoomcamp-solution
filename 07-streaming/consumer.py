from kafka import KafkaConsumer
from model import ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-console',
    value_deserializer=ride_deserializer
)

print(f"Listening to {topic_name}...")

count = 0
for message in consumer:
    ride = message.value
    if ride.trip_distance > 5:
        count += 1
        print(f"Ride {count}: {ride}")

consumer.close()
