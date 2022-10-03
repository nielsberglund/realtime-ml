# import the libraries
import json
import socket
import numpy as np
import pandas as pd
from confluent_kafka import Producer
from pathlib import Path
import datetime

# set up some variables
adclicktopic = "adclicks"

# get the path to the data file
# we use the original file with adclicks
base_path = Path(__file__).parent
model_path = (base_path / "../../data/advertising.csv").resolve()

df = pd.read_csv(model_path)
event_df = df.drop(labels=['Ad Topic Line','City','Country','Timestamp','Clicked on Ad'], axis=1)

# loop the dataframe
numberRows = len(event_df) - 1

def create_event(row):
    # create the event
    event = {
        "timeSpent": float(event_df.iloc[row,0]),
        "age": int(event_df.iloc[row,1]),
        "areaIncome": float(event_df.iloc[row,2]),
        "dailyUsage": float(event_df.iloc[row,3]),
        "isMale": int(event_df.iloc[row,4]),
        "eventTime": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    }
    
    print(event)

    # return the event
    return json.dumps(event)

for i in range(numberRows):
    print(create_event(i))
   

print("Hello World")
#68.95   35     61833.90                256.09     0

# load the data frame
# def load_and_return_df():
#     # load the data frame
#     df = pd.read_csv(model_path)
#     # return the data frame
#     return df

# df = load_and_return_df()

# print(df[0:1:10])