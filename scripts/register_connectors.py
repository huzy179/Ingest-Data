import urllib.request
import urllib.error
import json
import time
import os

CONNECT_URL = "http://localhost:8083/connectors"
CONNECTORS_DIR = "infrastructure/connectors"
CONNECTOR_FILES = [
    "postgres-source.json",
    "mongodb-source.json",
    "s3-sink.json"
]

def wait_for_connect():
    print("Waiting for Kafka Connect REST API to be ready at http://localhost:8083...")
    retries = 30
    while retries > 0:
        try:
            req = urllib.request.Request("http://localhost:8083/")
            with urllib.request.urlopen(req, timeout=3) as response:
                if response.status == 200:
                    print("Kafka Connect is ready!")
                    return True
        except Exception:
            pass
        retries -= 1
        time.sleep(5)
    print("Error: Kafka Connect did not start in time.")
    return False

def register_connector(filename):
    filepath = os.path.join(CONNECTORS_DIR, filename)
    if not os.path.exists(filepath):
        print(f"Error: Configuration file not found at {filepath}")
        return
        
    with open(filepath, "r", encoding="utf-8") as f:
        config_data = json.load(f)
        
    name = config_data.get("name")
    config = config_data.get("config")
    
    if not name or not config:
        print(f"Error: Invalid JSON structure in {filename}")
        return
        
    # Check if connector exists
    check_url = f"{CONNECT_URL}/{name}"
    exists = False
    try:
        req = urllib.request.Request(check_url)
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                exists = True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            exists = False
        else:
            print(f"HTTP Error checking {name}: {e.code} - {e.reason}")
            return
    except Exception as e:
        print(f"Error checking {name}: {e}")
        return
        
    if exists:
        # Update existing connector configuration
        update_url = f"{check_url}/config"
        print(f"Connector '{name}' already exists. Updating configuration...")
        try:
            req = urllib.request.Request(
                update_url,
                data=json.dumps(config).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="PUT"
            )
            with urllib.request.urlopen(req) as response:
                if response.status in (200, 201):
                    print(f"Successfully updated connector '{name}'!")
        except Exception as e:
            print(f"Failed to update connector '{name}': {e}")
    else:
        # Register new connector
        print(f"Registering new connector '{name}'...")
        try:
            req = urllib.request.Request(
                CONNECT_URL,
                data=json.dumps(config_data).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req) as response:
                if response.status in (200, 201):
                    print(f"Successfully registered connector '{name}'!")
        except Exception as e:
            print(f"Failed to register connector '{name}': {e}")

def main():
    if not wait_for_connect():
        return
        
    for filename in CONNECTOR_FILES:
        print(f"\nProcessing {filename}...")
        register_connector(filename)
        
if __name__ == "__main__":
    main()
