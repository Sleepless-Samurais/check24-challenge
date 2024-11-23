import requests
import time

url = "http://backend:80/api/offers"


# if __name__ == "__main__":
#     # Wait to ensure the server is up and running
#     time.sleep(5)
#     print("Starting")
#     for i in range(3):
#         print(f"{i} of 3:")
#         response = requests.get(url)
#         subprocess.run(["echo", response.text])
#         time.sleep(5)

import json
import requests
import time
import threading
import subprocess

# Define the endpoint
endpoint = "http://backend:80/api/offers"  # Replace with your actual endpoint

# Read the JSON lines from a file
# Assuming the file is named 'input.jsonl'
filename = "data/output.jsonl"

# Read and parse the JSON lines
requests_list = []
with open(filename, "r") as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                data = json.loads(line)
                requests_list.append(data)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {line}\n{e}")

# Check if any valid requests were founsubprocess.run(["echo", f"Sent GET request with params {params}, response status code: {response.status_code}"])

# Sort the requests by timestamp
requests_list.sort(key=lambda x: int(x["timestamp"]))

# Get the base timestamp (first request starts immediately)
base_timestamp = int(requests_list[0]["timestamp"])

# Prepare the list of requests with delays
scheduled_requests = []
for req in requests_list:
    timestamp = int(req["timestamp"])
    delay_ns = timestamp - base_timestamp
    delay_s = delay_ns / 1e9  # Convert nanoseconds to seconds
    scheduled_requests.append({"delay": delay_s, "request": req})


# Function to send the request
def send_request(req):
    requestType = req["requestType"]
    try:
        if requestType == "READ":
            params = req.get("search_config", {})
            response = requests.get(endpoint, params=params)
            subprocess.run(
                [
                    "echo",
                    f"Sent GET request with params {params}, response status code: {response.status_code}",
                ]
            )
        elif requestType == "PUSH":
            data = req.get("write_config", {})
            response = requests.post(endpoint, json=data)
            subprocess.run(
                [
                    "echo",
                    f"Sent POST request with data {data}, response status code: {response.status_code}",
                ]
            )
        elif requestType == "DELETE":
            response = requests.delete(endpoint)
            subprocess.run(
                [
                    "echo",
                    f"Sent DELETE request, response status code: {response.status_code}",
                ]
            )

        else:
            subprocess.run(["echo", f"Unknown requestType: {requestType}"])
    except Exception as e:
        subprocess.run(["echo", f"Error sending {requestType} request: {e}"])


subprocess.run(["echo", "Starting"])
time.sleep(5)
subprocess.run(["echo", "finished waiting"])

# Schedule the requests
for scheduled in scheduled_requests:
    delay = scheduled["delay"]
    req = scheduled["request"]
    threading.Timer(delay, send_request, args=(req,)).start()

subprocess.run(["echo", "All requests have been scheduled."])

# Keep the main thread alive until all requests have been sent
# Calculate the total duration
total_duration = scheduled_requests[-1]["delay"] + 1  # Add 1 second buffer
time.sleep(total_duration)
