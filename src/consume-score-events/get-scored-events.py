
import json

from confluent_kafka import Consumer

# code to run the application

# set up some variables
topic = "scored_events"

def init_consumer():
    # set the consumer configuration
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "consume-scored-events-4",
        "auto.offset.reset": "earliest"
    }
    # create the consumer
    consumer = Consumer(conf)
    # subscribe to the topic
    consumer.subscribe([topic])
    return consumer

def consume_events(consumer):
    # set up a loop to keep consuming
    input("Press any key to continue...")
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

        probability = event["probability"]

        if probability < 0.3:
            print(f'Low probability: {probability}')
        elif probability > 0.3 and probability < 0.7:
            print(f'Medium probability: {probability}')
        else:
            print(f'High probability: {probability}')

        print(event)

consumer = init_consumer()
consume_events(consumer)
print("We are done")            