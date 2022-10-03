# publish adclick events to a Kafka topic
# the events are published as JSON strings

# import the libraries
import random
import pandas as pd
import json
import socket
import datetime
import time
from confluent_kafka import Producer
from pathlib import Path

# set up some variables
adclicktopic = "adclicks"

minLatency = 500
maxLatency = 1000
doLoop = False

# get the path to the data file
# we use the original file with adclicks
base_path = Path(__file__).parent
data_path = (base_path / "../../data/advertising.csv").resolve()

# read in the data file
df = pd.read_csv(data_path)
event_df = df.drop(labels=['Ad Topic Line','City','Country','Timestamp','Clicked on Ad'], axis=1)

# get the number of rows
numberRows = len(event_df) - 1

# create the event
# we have to cast pandas data types to"normal" types
# otherwise, the JSON serialization will fail
def create_event(row):
    # create the event
    event = {
        "eventId": row, # for "giggles" we use the row number as the event ID
        "timeSpent": float(event_df.iloc[row,0]),
        "age": int(event_df.iloc[row,1]),
        "areaIncome": float(event_df.iloc[row,2]),
        "dailyUsage": float(event_df.iloc[row,3]),
        "isMale": int(event_df.iloc[row,4]),
        "eventTime": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    }
    # return the event
    return json.dumps(event)

# code to initialise the producer
def init_producer():
    # set the producer configuration
    conf = {
        "bootstrap.servers": "localhost:9092",
        "client.id": socket.gethostname()
    }
    # create the producer
    producer = Producer(conf)
    return producer

# code to set up a callback function to handle the delivery report
def delivery_report(err, msg):
    # if there was an error, print it
    if err is not None:
        print("Message delivery failed: {}".format(err))
    # otherwise, print the message
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

# publish the adclick event to the topic
# the event is published as a string
def publish_event(producer, topic, event, msgKey):
    # publish the model
    producer.produce(topic, key=msgKey, value=event, callback=delivery_report)
    # flush the producer
    producer.flush() 

# initialise the producer
producer = init_producer()    

# create events and publish them
for i in range(numberRows):
    event = create_event(i)

    if not doLoop :
        val = input("Press 'Y' to continue, 'N' to exit.")
    
        if val == "Y":
            publish_event(producer, adclicktopic, event, str(i))
         
        if val == "N":
          break
    else:
        publish_event(producer, adclicktopic, event, str(i))
        time.sleep(random.randint(minLatency, maxLatency)/1000)

print("Done")


