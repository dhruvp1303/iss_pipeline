import requests
import time
from datetime import datetime, timezone

url = "https://api.wheretheiss.at/v1/satellites/25544"

print("\nStarting ISS poller. Press Ctrl+C to stop.\n")

while True:
    try:
        response = requests.get(url, timeout=10)
        data = response.json()

        # readable time
        when = datetime.fromtimestamp(data["timestamp"], timezone.utc).strftime("%H:%M:%S")        
        print(f"[{when} UTC]  lat={data['latitude']:.2f}  lon={data['longitude']:.2f}  speed={data['velocity']:.0f} km/h")

    except Exception as e:
        # If a request fails, don't crash — log it and keep going
        print(f"Request failed: {e}  (will retry)")

    time.sleep(5)  # wait 5 seconds, then poll again