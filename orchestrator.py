import json
import time
import statistics
from kafka import KafkaConsumer,KafkaProducer
from datetime import datetime 
import threading
import uuid
# Initialize Kafka consumer for receiving metrics from driver nodes
consumer = KafkaConsumer('metrics', bootstrap_servers='localhost:9092',api_version=(0,11,5))
consumer_register = KafkaConsumer('register',bootstrap_servers='localhost:9092',api_version=(0,11,5))
consumer_heartbeat= KafkaConsumer('heartbeat',bootstrap_servers='localhost:9092',api_version=(0,11,5))

producer = KafkaProducer(bootstrap_servers='localhost:9092',api_version=(0,11,5))
driver_count =1 #number of driver nodes 
c=0
for line in consumer_register:
   line= line.value.decode('utf-8')
   data2 = json.loads(line)
   data3 = json.dumps(data2,indent=4)
   print(data3)
   print("\n")
   print("Driver Node with NodeID:",data2["node_id"],"registered")
   print("\n")
   c=c+1
   if(c==driver_count):
    break
   
   
print("Proceed")
t=0
avts=int(input("Enter 1 for Tsunami 2 for Avalanche testing : "))
if(avts==1):
    t=int(input("Enter time for Tsunami : "))

#test_message={"test_mode":avts,
              #"time":t}
tid=str(uuid.uuid4())            
test_message=  {
  "test_id": tid,
  "test_mode": avts,
  "test_message_delay": 1,
  "message_count_per_driver": 10,
  "time":t
}
trigger_message={
  "test_id": tid,
  "trigger": "YES"
}
print("Before producer")
producer.send('test_mode', json.dumps(test_message).encode('utf-8'))
if(input("Enter yes to start testing : ")=="yes"):
    producer.send('trigger', json.dumps(trigger_message).encode('utf-8'))

def calculate_statistics():
    print("calculating..")
    print(time)
    if timedict:
        mean_of_means = sum(node_data['metrics']['mean'] for node_data in timedict.values()) / len(timedict)
        max_of_maxes = max(node_data['metrics']['max'] for node_data in timedict.values())
        
        median_values = [node_data['metrics']['median'] for node_data in timedict.values()]
        median = statistics.median(median_values)
        min_of_mins = min(node_data['metrics']['min'] for node_data in timedict.values())

        print("Minimum of all min latencies:", min_of_mins)
        print("Mean of all mean latencies:", mean_of_means)
        print("Maximum of all max latencies:", max_of_maxes)
        print("Median of all median latencies:", median)

     




timedict={}

if __name__ == '__main__':
    # Run the display_statistics function in a separate thread or process
    # To continuously update and display response time statistics
    #display_statistics()

    # Consume metrics from Kafka
    print("orchestrator")
    def tester():
        for message in consumer:
            #print(message)
            data = json.loads(message.value)
            print(json.dumps(data,indent=4))
            if(data["type"]=="metrics"):
                met= data['metrics']
                key=data["node_id"]
                timedict[key]=data
                print("timedictt",json.dumps(timedict,indent=4))
                calculate_statistics()
                #response_times.append(response_time)
    def heart():
        for message in consumer_heartbeat:
            data = json.loads(message.value)

            if(data["type"]=="heartbeat"):
                print("\n\n",data["node_id"],"is alive \n\n")

    thread1 = threading.Thread(target=heart)
    thread2 = threading.Thread(target=tester)
    thread1.start()
    thread2.start()
    
