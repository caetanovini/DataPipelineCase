import requests
import json

def fetch_brewery_data():
    url = "https://api.openbrewerydb.org/breweries"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        with open('data_architecture/data_sources/bronze_layer.json', 'w') as f:
            json.dump(data, f)
            print("data sucessed imported")
    else:
        raise Exception(f"Error fetching data from API: {response.status_code}")

if __name__ == "__main__":
    fetch_brewery_data()