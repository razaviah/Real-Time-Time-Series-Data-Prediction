# from confluent_kafka import Producer, KafkaError
# from confluent_kafka.admin import AdminClient, NewTopic
# import json
# import time
# import os

# def delivery_report(err, msg, msg_num):
#     if err is not None:
#         print('Message delivery failed: {}'.format(err))
#     else:
#         print('Message {} delivered to {}'.format(msg_num, msg.topic()))

# def create_ride():
#     # Simulate ride data, replace with your logic
#     return {
#         "rideId": 123,
#         "kir-bere-koja": "too kose ammeye jendam"
#     }

# conf = {
#     'bootstrap.servers': 'kafka:9092', # '44.217.134.171:9092',  # Replace with your Kafka broker
# }

# # Create a Kafka admin client to manage topics
# admin_client = AdminClient(conf)

# # Define the "Rides" topic
# topic_name = 'Rides'
# new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)

# # Create the "Rides" topic if it doesn't exist
# try:
#     admin_client.create_topics([new_topic])
# except KafkaError as e:
#     # Ignore if the topic already exists
#     if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
#         raise

# # Create the Kafka producer
# producer = Producer(conf)

# message_counter = 0

# while True:
#     message_counter += 1
#     ride = create_ride()
#     producer.produce(topic_name, key=str(ride['rideId']), value=json.dumps(ride), callback=lambda err, msg, msg_num=message_counter: delivery_report(err, msg, msg_num))
#     producer.poll(0)
#     time.sleep(0.1)  # Control the rate of data production

# producer.flush()














# from confluent_kafka import Producer, KafkaError
# from confluent_kafka.admin import AdminClient, NewTopic
# import requests
# import json
# import time
# import os

# # Put your API key here
# api_key = "%%%%%%%%%%%%%%%%%%%%%%%%"

# def delivery_report(err, msg, msg_num):
#     if err is not None:
#         print('Message delivery failed: {}'.format(err))
#     else:
#         print('Message {} delivered to {}'.format(msg_num, msg.topic()))

# def get_stock_data():
#     # Replace with your Twelve Data endpoint and API key
#     # url = f"https://api.twelvedata.com/time_series?symbol=AAPL&interval=30min&outputsize=60&apikey={api_key}"
#     url = f"https://api.twelvedata.com/quotes/price?symbol=AAPL&apikey={api_key}"
    
#     response = requests.get(url)
#     return response # .json()

# conf = {
#     'bootstrap.servers': 'kafka:9092', # Replace with your Kafka broker
# }

# # Create a Kafka admin client to manage topics
# admin_client = AdminClient(conf)

# # Define the "Stocks" topic
# topic_name = 'Stocks'
# new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)

# # Create the "Stocks" topic if it doesn't exist
# try:
#     admin_client.create_topics([new_topic])
# except KafkaError as e:
#     # Ignore if the topic already exists
#     if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
#         raise

# # Create the Kafka producer
# producer = Producer(conf)

# message_counter = 0

# while True:
#     message_counter += 1
#     stock_data = get_stock_data()
#     # producer.produce(topic_name, value=json.dumps(stock_data), callback=lambda err, msg, msg_num=message_counter: delivery_report(err, msg, msg_num))
#     producer.produce(topic_name, value=stock_data, callback=lambda err, msg, msg_num=message_counter: delivery_report(err, msg, msg_num))
#     producer.poll(0)
#     time.sleep(12)  # Fetch stock data every 12 seconds

# producer.flush()












from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import websocket
import json
import os

def delivery_report(err, msg, msg_num):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message {} delivered to {}'.format(msg_num, msg.topic()))

def on_open(ws):
    print("WebSocket opened")
    subscription_request = {
        "action": "subscribe",
        "params": {"symbols": "AAPL"}
    }
    ws.send(json.dumps(subscription_request))

def on_message(ws, message):
    global message_counter
    message_counter += 1
    print(f"Received: {message}")
    producer.produce(topic_name, value=message, callback=lambda err, msg, msg_num=message_counter: delivery_report(err, msg, msg_num))

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, code, reason):
    print("WebSocket closed")

conf = {
    'bootstrap.servers': 'kafka:9092', # Replace with your Kafka broker
}

# Create a Kafka admin client to manage topics
admin_client = AdminClient(conf)

# Define the "RealtimeStock" topic
topic_name = 'RealtimeStock'
new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)

# Create the "RealtimeStock" topic if it doesn't exist
try:
    admin_client.create_topics([new_topic])
except KafkaError as e:
    # Ignore if the topic already exists
    if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
        raise

# Create the Kafka producer
producer = Producer(conf)

message_counter = 0

# Replace YOUR_API_KEY with your Twelve Data API key
api_key = "%%%%%%%%%%%%%%%%%%%%%%%%"
ws_url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={api_key}"

ws = websocket.WebSocketApp(ws_url,
                            on_open=on_open,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)

ws.run_forever()

# Ensure any remaining messages are sent
producer.flush()
