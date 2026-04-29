from kafka import KafkaProducer
from kafka.errors import KafkaError
import csv
import json


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

FILE_PATH = 'data/2019-Nov.csv'
TARGET_TABLE = "raw_events"

try:
    with open(FILE_PATH, 'r', encoding='utf-8') as file:
        csv_content = csv.DictReader(file)

        for count, content in enumerate(csv_content):
            producer.send(TARGET_TABLE, value=content)

            if count%10000==0:
                print(f"Counter : {count}")

except FileNotFoundError:
    print( "File is not found, check FILE_PATH")


finally:
    producer.flush()
    producer.close()





