import pandas as pd
import time
from kafka import KafkaProducer
import dataclasses
import json
from model import Ride, ride_from_row, ride_serializer

url = "./green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
df = pd.read_parquet(url, columns=columns)

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

t0 = time.time()

topic_name = 'green-trips'

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")

producer.flush()
print(f"Read {len(df)} rows")

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
