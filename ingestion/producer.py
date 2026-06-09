import requests
import time
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

# --- Kafka setup ---
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",          # the door to our Kafka broker
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # turn dict -> JSON bytes
)

TOPIC = "iss-positions"
URL = "https://api.wheretheiss.at/v1/satellites/25544"

print("Producing ISS positions to Kafka. Ctrl+C to stop.\n")

while True:
    try:
        # 1. Extract — fetch live position
        resp = requests.get(URL, timeout=20)
        data = resp.json()

        # 2. Shape the message we care about
        message = {
            "satellite_id": data["id"],
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "altitude_km": data["altitude"],
            "velocity_kmh": data["velocity"],
            "timestamp": data["timestamp"],
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        # 3. Send to Kafka
        producer.send(TOPIC, value=message)

        when = datetime.fromtimestamp(data["timestamp"], timezone.utc).strftime("%H:%M:%S")
        print(f"Sent → [{when} UTC] lat={message['latitude']:.2f} lon={message['longitude']:.2f}")

    except Exception as e:
        print(f"Failed: {e}  (retrying)")

    time.sleep(10)