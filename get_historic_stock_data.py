import requests
import json
from datetime import timedelta, datetime
from time import sleep


counter = 0
api_key = "%%%%%%%%%%%%%%%%%%%%%%%%"
def fetch_stock_data(symbol, start_date, end_date, counter):
    all_data = []
    current_start_date = start_date

    while current_start_date < end_date:
        # Skip weekends
        if current_start_date.weekday() >= 5:
            current_start_date += timedelta(days=1)
            continue

        current_end_date = current_start_date + timedelta(days=10) # 10 days of data
        url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1min&start_date={current_start_date}&end_date={current_end_date}&apikey={api_key}"
        response = requests.get(url)
        all_data.extend(response.json()['values'])
        current_start_date = current_end_date
        counter += 1
        print(f"got the batch for {counter}")
        sleep(10)

    return all_data

symbols = ["AAPL"]
start_date = datetime.strptime("2021-08-23", "%Y-%m-%d")
end_date = datetime.strptime("2023-08-23", "%Y-%m-%d")

all_data = {}

for symbol in symbols:
    all_data[symbol] = fetch_stock_data(symbol, start_date, end_date, counter)

with open('stock_data_AAPL_21,aug,23-23,aug,23.json', 'w') as file:
    json.dump(all_data, file)
