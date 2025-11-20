# producer_pg_to_kafka.py
import psycopg2
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

cursor.execute("""
ALTER TABLE user_logins
    ADD COLUMN IF NOT EXISTS sent_to_kafka BOOLEAN;
""")

cursor.execute("""
ALTER TABLE user_logins
    ALTER COLUMN sent_to_kafka SET DEFAULT FALSE;
""")

cursor.execute("""
UPDATE user_logins
SET sent_to_kafka = FALSE
WHERE sent_to_kafka IS NULL;
""")
conn.commit()


cursor.execute("""
    SELECT id, username, event_type, extract(epoch FROM event_time)
    FROM user_logins
    WHERE sent_to_kafka IS NOT TRUE
    ORDER BY event_time
""")
rows = cursor.fetchall()

for row in rows:
    row_id = row[0]
    data = {
        "id": row_id,
        "user": row[1],
        "event": row[2],
        "timestamp": float(row[3]),
        "sent_to_kafka": True
    }

  
    producer.send("user_events_to_clickhouse", value=data)
    producer.flush()

    
    cursor.execute(
        "UPDATE user_logins SET sent_to_kafka = TRUE WHERE id = %s",
        (row_id,)
    )
    conn.commit()

    print("Sent:", data)
    time.sleep(0.5)

cursor.close()
conn.close()
producer.close()