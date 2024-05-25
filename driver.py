#!/usr/bin/env python3
import sys

import requests
import json
import time
import socket
import uuid
from kafka import KafkaProducer
from kafka import KafkaConsumer
import threading
import statistics

topic_names=sys.argv[1:]
KafkaIP = topic_names[1]
OrchestratorIP = topic_names[2]
print("Kafka IP:",KafkaIP,"OrcherstatorIP:",OrchestratorIP)
print("\n\n")
# Initialize Kafka producer for sending metrics
consumer = KafkaConsumer('test_mode', bootstrap_servers=KafkaIP, api_version=(0,11,5))
consumer_trigger= KafkaConsumer('trigger', bootstrap_servers=KafkaIP, api_version=(0,11,5))

producer = KafkaProducer(bootstrap_servers=KafkaIP,api_version=(0,11,5))

#producer_register = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(0,11,5))
count=0
def tsunami(tsunami_time):   
    #count=0
    global count 
    
    while (count<numb):
        start_time = time.time()
        try:
            response = requests.get('http://127.0.0.1:8080/test')
            end_time = time.time()
            response_time = (end_time - start_time) * 1000  # Convert to milliseconds
            response_times.append(response_time)


        except requests.RequestException as e:
            # Handle exceptions or errors with the request
            print("Request Exception:", e)
        time.sleep(tsunami_time)  # Adjust the interval as needed
        count=count+1
        print(count)

def avalanche():
    #count=0
    global count 
    while (count<numb):
        print("avalnche testing")
        start_time = time.time()
        try:
            response = requests.get('http://127.0.0.1:8080/test')
            end_time = time.time()
            response_time = (end_time - start_time) * 1000  # Convert to milliseconds
            response_times.append(response_time)

            # Send response time metrics to Kafka

       
        except requests.RequestException as e:
            # Handle exceptions or errors with the request
            print("Request Exception:", e)
        count=count+1
        print(count)
        
        #time.sleep(1)  # Adjust the interval as needed
def heartbeatengine():
    while (count<numb):
        print("sending heartbeats")
        producer.send('heartbeat', json.dumps(heartbeat).encode('utf-8'))
        time.sleep(1)
    print("done\n\n\n\n\n\n\n\n\n")
    exit()
    thread4.stop()



def metricsender():
    report_id=str(uuid.uuid4())  

    print("metric sender")
    while True:
        metrics_message = {
            'type':'metrics',
            'node_id': unique_node_id,
            'test_id': test_id,
            'report_id': report_id,
            'metrics': {
                'response_time': response_time,
                'mean':statistics.mean(response_times) if response_times else 0,
                'median':statistics.median(response_times) if response_times else 0,
                'min':min(response_times) if response_times else 0,
                'max':max(response_times) if response_times else 0,
                'count':count,

            }
        }
        #print(metrics_message)
        #print(json.dumps(metrics_message).encode('utf-8'))
        metadata=producer.send('metrics', json.dumps(metrics_message).encode('utf-8'))
        #producer.send('metrics', json.dumps(heartbeat).encode('utf-8'))
        time.sleep(1)
        #print(metadata)


response_times = []
response_time=0

node_ip = socket.gethostbyname(socket.gethostname())
unique_node_id = str(uuid.uuid4())
print(node_ip,unique_node_id)
driver_registration_message = {
    "type":"registration",
    "node_id": unique_node_id,  # Replace with a unique identifier for the driver node
    "node_IP": node_ip,  # Replace with the actual IP address
    "message_type": "DRIVER_NODE_REGISTER",
}
#count=0
heartbeat={
   'type':'heartbeat',
  "node_id": unique_node_id,
  "heartbeat": "YES",
  #"count":count
}
print(driver_registration_message)
producer.send('register', json.dumps(driver_registration_message).encode('utf-8'))
#producer.send('register',"youuu why is it not working".encode('utf-8'))

print("registered")
thread1 = threading.Thread(target=avalanche)
thread3 = threading.Thread(target=heartbeatengine)
thread4 = threading.Thread(target=metricsender)

numb=1
thread3.start()

print("waiting for test type and trigger messgae yes")

for message in consumer:
    
    #print(message)
    data = json.loads(message.value)
    test_type= data['test_mode']
    tsunami_time= data['time']
    test_id=data['test_id']
    numb=data['message_count_per_driver']
    for message in consumer_trigger:
        data = json.loads(message.value)
        print(data)
        if data["trigger"]=="YES":
            print("starting test")
            break
    print(test_type)
    if(test_type==1):
        #tsunami(tsunami_time)
        #thread2 = threading.Thread(target=tsunami(tsunami_time))
        thread2 = threading.Thread(target=tsunami, args=(tsunami_time,))

        thread2.start()
        #thread3.start()
        thread4.start()

    else:
        thread1.start()
        #thread3.start()  
        thread4.start()
