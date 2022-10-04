import numpy as np
import pandas as pd
from confluent_kafka import Consumer, Producer
import sklearn
import pickle
import socket
import sys
import random
import time
import json

# code to run the application

# set up some variables
eventTopic = "event_to_score"
scoredTopic = "scored_events"

# code to set up a callback function to handle the delivery report
def delivery_report(err, msg):
    # if there was an error, print it
    if err is not None:
        print("Message delivery failed: {}".format(err))
    # otherwise, print the message
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

# code to initialise the consumer
def init_consumer():
    # set the consumer configuration
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "score-events-5",
        "auto.offset.reset": "latest"
    }
    # create the consumer
    consumer = Consumer(conf)
    # subscribe to the topic
    consumer.subscribe([eventTopic])
    return consumer

# code to initialise the producer
def init_publisher():
    # set the producer configuration
    conf = {
        "bootstrap.servers": "localhost:9092",
        "client.id": socket.gethostname()
    }
    # create the producer
    producer = Producer(conf)
    return producer

# code to consume the events
def consume_events(consumer, producer):
    # set up a loop to keep consuming
    while True:
        # poll for a message
        msg = consumer.poll(1.0)
        # if there is a message
        if msg is None:
            continue
        # if there is an error
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        event = json.loads(msg.value())
        # call the scoring function
        score_event(event, producer)

# code to score the event
def score_event(event, producer):
    # load the model
    lr_model = pickle.loads(event["MODEL"].encode('latin1'))

    pd.set_option('mode.chained_assignment', None)
    # convert the event to a dataframe
    df = pd.DataFrame([event])
    # get the features from the dataframe
    features = df[["TIMESPENT", "AGE", "AREAINCOME", "DAILYUSAGE", "ISMALE"]]
    # get the prediction
    probability = lr_model.predict_proba(features.to_numpy().reshape(1, -1))
    # rename the columns
    features.rename(columns={"TIMESPENT": "timeSpent", "AGE": "age", "AREAINCOME": "areaIncome", "DAILYUSAGE": "dailyUsage", "ISMALE": "isMale"}, inplace=True)
    # add the probability to the event
    features["probability"] = probability[0][1]
    features["modelId"] = event["MODELID"]
    features["modelName"] = event["MODELNAME"]
    features["eventTime"] = event["EVENTTIME"]
    eventId = event["EVENTID"]
    features["eventId"] = eventId
    evt = features.to_json(orient="records").removeprefix("[").removesuffix("]")
    # publish the event
    publish_event(producer, scoredTopic, evt, str(eventId) )
 

# coode to publish the event
def publish_event(producer, topic, event, msgKey):
    # publish the event
    producer.produce(topic, event, msgKey, callback=delivery_report)
    # flush the producer
    producer.flush()  

consumer = init_consumer()
producer = init_publisher()
consume_events(consumer, producer)
print("We are done")    