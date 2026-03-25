import requests
import json
import time
import sys

def test_optimize():
    url = "http://localhost:8000/api/optimize"
    payload = {
        "age": 30,
        "gender": "male",
        "weight_kg": 75,
        "height_cm": 180,
        "activity_level": "moderately_active",
        "goal": "maintenance"
    }
    
    print(f"Testing {url}...")
    try:
        start_time = time.time()
        response = requests.post(url, json=payload, timeout=90) # Long timeout for optimization
        duration = time.time() - start_time
        
        print(f"Status Code: {response.status_code}")
        print(f"Duration: {duration:.2f}s")
        
        if response.status_code == 200:
            data = response.json()
            print("Response JSON keys:", data.keys())
            print(f"Optimization Status: {data.get('status')}")
            print(f"Calories: {data.get('totals', {}).get('calories', 'N/A')}")
            
            if data.get('status') in ['Optimal', 'Feasible']:
                print("SUCCESS: valid solution returned.")
                return True
            else:
                 print("FAILURE: Optimization failed to find solution.")
                 return False
        else:
            print(f"FAILURE: Server returned {response.text}")
            return False
            
    except Exception as e:
        print(f"FAILURE: Request Error: {e}")
        return False

if __name__ == "__main__":
    # Small delay to allow server to be ready if run immediately after start
    print("Waiting 5s for server warmup...")
    time.sleep(5)
    success = test_optimize()
    sys.exit(0 if success else 1)
