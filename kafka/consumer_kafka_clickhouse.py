# consumer_to_clickhouse.py
from kafka import KafkaConsumer
import json
import clickhouse_connect

consumer = KafkaConsumer(
    "user_events_to_clickhouse",
    bootstrap_servers="localhost:9092",
    auto_offset_reset='earliest',
    group_id="clickhouse-consumer",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='user',
    password='strongpassword'
)

client.command("""
CREATE TABLE IF NOT EXISTS user_logins (
    username String,
    event_type String,
    event_time DateTime,
    sent_to_kafka Bool
) ENGINE = MergeTree()
ORDER BY event_time
""")

for message in consumer:
    data = message.value
    print("Received:", data)

    sent_flag = 1 if data.get("sent_to_kafka") else 0

    client.command(
        f"""
        INSERT INTO user_logins (username, event_type, event_time, sent_to_kafka)
        VALUES ('{data['user']}', '{data['event']}', toDateTime({data['timestamp']}), {sent_flag})
        """
    )