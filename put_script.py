import requests
import random
import time
import uuid
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define the API endpoint
api_url = "http://127.0.0.1:7878/"  # Adjust to your endpoint and port

# Function to generate a JSON object with 5 columns
def generate_json_object():
    return {
        "column1": random.randint(1, 100),
        "column2": (datetime.now() + timedelta(days=random.randint(-365, 365))).isoformat(),
        "column3": str(uuid.uuid4()),  # Unique identifier
        "column4": str(random.randint(1, 10)),  # Random number between 1 and 10
        "column5": random.randint(100, 1000),  # Random number between 100 and 1000
    }

# Function to send a single JSON object to the API endpoint
def send_json_object(i):
    json_object = {
        "key": i,
        "value": generate_json_object()
    }
    
    time.sleep(0.1)
    
    try:
        # Send the JSON to the API endpoint
        response = requests.put(api_url, json=json_object)

        # Return a success message with the response status code
        return f"Sent object with key={i}; Response: {response.status_code} - {response.text}"
    except requests.exceptions.RequestException as e:
        # Return an error message if there's a problem
        return f"Error sending object with key={i}; Details: {str(e)}"

# Define a thread pool to execute tasks concurrently
with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust the number of workers as needed
    # Schedule the sending of 100 JSON objects concurrently
    futures = [executor.submit(send_json_object, i) for i in range(1, 101)]

    # Process the results as they complete
    for future in as_completed(futures):
        # Print the result or error message
        print(future.result())

print("All 100 objects have been sent.")
