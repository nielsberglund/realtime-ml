from confluent_kafka import Producer
import socket
import sys
import gameplay
import random
import time

numberLoops = 50
minLatency = 500
maxLatency = 1000
doLoop = True
numMsg = 0
topic = "gameplay"

# Kafka

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}


producer = Producer(conf)

input("We are ready, press the enter key to continue")

def generateAndPublish(topic) :
    keyVal, jsonObj = gameplay.generateGamePlay()
    producer.produce(topic, key=str(keyVal), value=jsonObj, callback=acked)
    producer.poll(1)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
      if (numMsg < 5 or numMsg % 100 == 0) :
          print("Message produced: %s." % (str(msg)))



if not doLoop :
  while(True) :
    val = input("Press 'Y' to continue, 'N' to exit.")
    
    if val == "Y":
      generateAndPublish(topic)
         
    if val == "N":
      break

if(doLoop):
  for i in range(numberLoops) :
    numMsg += 1
    latency = random.randint(minLatency, maxLatency)
    generateAndPublish(topic)
    time.sleep(latency / 1000)
    
    
producer.poll(1)
producer.flush()
input("Press the enter key to exit")