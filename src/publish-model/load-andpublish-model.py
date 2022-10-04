# here we load the model and publish it to the topic

# import the necessary packages
from pathlib import Path
import pickle
import json
import socket

# import the Producer from the confluent_kafka library
from confluent_kafka import Producer

# set up some variables
# the path to the model file
base_path = Path(__file__).parent
model_path = (base_path / "../../models/adclick.pkl").resolve()

# the topic to publish to
topic = "models"

# load the model and return as string
def load_and_return_model():
    # load the model
    lr_model = pickle.load(open(model_path, "rb"))
    # return the model as string
    return pickle.dumps(lr_model).decode('latin1')

# generate an event based on the model string
def generate_event(model_string):
    # create the event
    event = {
        "modelId": 1,
        "modelName": "adclick-logreg",
        "model": model_string
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

# publish the model to the topic
# the model is published as a string
def publish_model(producer, topic, event, msgKey):
    # publish the model
    producer.produce(topic, event, key=msgKey, callback=delivery_report)
    # flush the producer
    producer.flush() 

# main function
def main():
    # load the model
    model_string = load_and_return_model()
    # generate the event
    event = generate_event(model_string)
    # initialise the producer
    producer = init_producer()
    # publish the model
    publish_model(producer, topic, event, "1")

main()    