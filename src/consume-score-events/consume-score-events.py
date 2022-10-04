import numpy as np
import pandas as pd
import sklearn
import pickle
import socket
import sys
import random
import time
import json

from confluent_kafka import Consumer

# set up some variables
topic = "event_to_score"

# code to initialise the consumer
def init_consumer():
    # set the consumer configuration
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "score-events-18",
        "auto.offset.reset": "earliest"
    }
    # create the consumer
    consumer = Consumer(conf)
    # subscribe to the topic
    consumer.subscribe([topic])
    return consumer

# code to consume the events
def consume_events(consumer):
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
        # event = msg.value().decode('utf-8')
        event = json.loads(msg.value())
        # call the scoring function
        #score_event(msg.value().decode('utf-8'))
        score_event(event)

# code to score the event
def score_event(event):
    # load the model
    lr_model = pickle.loads(event["MODEL"].encode('latin1'))
    # convert the event to a dataframe
    df = pd.DataFrame([event])
    # get the features from the dataframe
    features = df[["TIMESPENT", "AGE", "AREAINCOME", "DAILYUSAGE", "ISMALE"]]
    # get the prediction
    prediction = lr_model.predict_proba(features.to_numpy().reshape(1, -1))
    # print the prediction
    print("Prediction: {}".format(prediction))
    # add the prediction to the event
    features["PROBABILITY"] = prediction[0][1]
    evt = features.to_json(orient="records")
    print (evt)
    evtString = json.dumps(evt)
    print (evtString)
    

consumer = init_consumer()
consume_events(consumer)
print("We are done")    