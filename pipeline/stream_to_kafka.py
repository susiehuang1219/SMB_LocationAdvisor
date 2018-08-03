from kafka import KafkaProducer # pip install kafka-python
import csv
import json
import time
from datetime import datetime

# Connect to Kafka
producer = KafkaProducer(bootstrap_servers='ec2-52-35-164-222.us-west-2.compute.amazonaws.com:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic = 'topic1'

with open('./TEST_yelp_business.csv', mode='r') as infile:
  reader = csv.reader(infile)
  mydict = {}
  for rows in reader:
    mydict['key'] = rows[0]
    mydict['name'] = rows[1]
    mydict['longitude'] = rows[2]
    mydict['latitude'] = rows[3]
    producer.send(topic, mydict)
    print ("Sent {0}".format(mydict))
    time.sleep(2)