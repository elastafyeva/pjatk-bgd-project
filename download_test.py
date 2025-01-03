# download_test.py
import requests

url = "https://jsonplaceholder.typicode.com/todos/1"
response = requests.get(url)

if response.status_code == 200:
    print("Downloaded data:")
    print(response.json())
else:
    print(f"Failed to download data. Status code: {response.status_code}")
