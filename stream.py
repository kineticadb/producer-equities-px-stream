import os
import sys
import random
import math
import time
import datetime
import json
import copy
import logging

from datetime import timezone
from time import strftime

from kafka import KafkaProducer

#######################################################################################
# ENVIRONMENT CONFIGS

KAFKA_PASS=os.getenv('KAFKA_SASL_PASS')
KAFKA_USER=os.getenv('KAFKA_SASL_USER')
KAFKA_BRKR=os.getenv('KAFKA_HOST')
KAFKA_PORT=os.getenv('KAFKA_PORT')
KAFKA_TOPIC=os.getenv('KAFKA_TOPIC')

SEED_FILE = "seed.csv"
TIME_BETWEEN_LOOPS = 1

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.WARN)

#######################################################################################
# CONNECTIONS

logging.info('Connecting to Kafka')
producer = KafkaProducer(bootstrap_servers=[f"{KAFKA_BRKR}:{KAFKA_PORT}"],
	sasl_plain_username = KAFKA_USER,
	sasl_plain_password = KAFKA_PASS,
	security_protocol="SASL_SSL",
	sasl_mechanism="PLAIN",
	value_serializer=lambda x:
	json.dumps(x).encode('utf-8'))
logging.info('Connecting to Kafka, completed')

px_initial = {}
with open(SEED_FILE, "r", encoding='utf-8-sig') as my_file:
	content = my_file.read()
	quotes = content.split("\n")
	for q in quotes:
		ticker, pxinit = q.split(",")
		px_initial[ticker] = float(pxinit)
px_now = copy.deepcopy(px_initial)

def main():
	

	while True:
		
		time.sleep(TIME_BETWEEN_LOOPS)

		q=[]
		push_stamp = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") #.isoformat()
		for c in px_now.keys():
			#print(f"Handling ticker: {c}")
			newprice = px_now[c] + (random.random()-0.5)/2
			px_now[c] = round(newprice, 2)
			msg = {
				"symbol": c,
				"px_last": px_now[c],
				"px_timestamp": push_stamp,
				"size": int(random.random() * 10000)
			}
			q.append(msg)
			producer.send(KAFKA_TOPIC, value=msg)
		producer.flush()

		print(f"{push_stamp}   Tick!  Disney: {px_now['DIS']} \t AIG: {px_now['AIG']} \t Goldman Sachs: {px_now['GS']} \t Accenture: {px_now['ACN']}")
		
		
if __name__ == "__main__":
    main()
