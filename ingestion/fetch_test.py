import requests

# Live ISS position. 25544 is the ISS's satellite ID.
url = "https://api.wheretheiss.at/v1/satellites/25544"
response = requests.get(url, timeout=10)

data = response.json()
print(f"\nStatus code: {response.status_code}")
print("\nISS right now:")
print("  Latitude: ", data["latitude"])
print("  Longitude:", data["longitude"])
print("  Altitude: ", data["altitude"], "km")
print("  Speed:    ", data["velocity"], "km/h")
print("  Timestamp:", data["timestamp"])