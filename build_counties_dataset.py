import requests
import json
import time
import re

# Function to fetch and process county data for a state
def get_counties_for_state(state_id):
    url = f"https://glorecords.blm.gov/search/getLookupData.aspx?searchType=survey&key={state_id}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Try to parse JSON directly first
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            print(f"  JSON parsing failed for {state_id}: {e}")
            
            # Fix common escape sequence issues
            try:
                # Fix invalid escape sequences by replacing single backslashes with double backslashes
                # where they're not part of valid JSON escape sequences
                text = re.sub(r'\\(?!["\\/bfnrt]|u[0-9a-fA-F]{4})', r'\\\\', response.text)
                data = json.loads(text)
                print(f"  Fixed JSON for {state_id}")
            except Exception as e2:
                print(f"  JSON repair failed: {e2}")
                # Save problematic response for debugging
                with open(f"error_{state_id}.json", "w") as f:
                    f.write(response.text)
                print(f"  Saved problematic response to error_{state_id}.json")
                return []
        
        # Extract counties and format them
        counties = []
        if "counties" in data and data["counties"]:
            for county in data["counties"]:
                counties.append({
                    "id": county[0],
                    "name": county[1]
                })
        
        return counties
    except Exception as e:
        print(f"Error fetching data for state {state_id}: {e}")
        return []

# Main function to process all states
def get_all_state_counties():
    # Load states from the JSON file
    try:
        with open("states.json", "r") as f:
            states = json.load(f)
    except Exception as e:
        print(f"Error loading states.json: {e}")
        return []
    
    result = []
    success_count = 0
    fail_count = 0
    
    for state in states:
        print(f"Processing {state['name']}...")
        
        # Create the state entry
        state_entry = {
            "state": state["name"],
            "abbreviation": state["id"],
            "counties": []
        }
        
        # Get and add counties
        counties = get_counties_for_state(state["id"])
        state_entry["counties"] = counties
        
        if counties:
            success_count += 1
        else:
            fail_count += 1
            
        # Add to results
        result.append(state_entry)
        
        # Add a delay to be nice to the server
        time.sleep(1)
    
    print(f"Completed: {success_count} states successful, {fail_count} states failed")
    return result

# Run the script
if __name__ == "__main__":
    all_data = get_all_state_counties()
    
    # Save to file
    with open("state_counties.json", "w") as f:
        json.dump(all_data, f, indent=2)
    
    print(f"Data saved to state_counties.json")