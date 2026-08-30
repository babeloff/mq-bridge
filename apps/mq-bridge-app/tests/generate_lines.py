#!/usr/bin/env python3
import time
import json
from datetime import datetime

filename = "input.txt"

print(f"Writing to {filename} every 2 seconds... Press Ctrl+C to stop.")

try:
    while True:
        data = {
            "timestamp": datetime.now().isoformat(),
            "message": "Hello from Python"
        }
        
        # Open in append mode
        with open(filename, "a") as f:
            f.write(json.dumps(data) + "\n")
            
        print(f"Wrote: {data['timestamp']}")
        # time.sleep(2)
except KeyboardInterrupt:
    print("\nStopped.")
