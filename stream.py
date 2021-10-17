import os
import sys
import random
import math
import time
import datetime
import json
import copy
import logging
import argparse

from datetime import timezone
from time import strftime

from recache import pull_quotes
from recache import pull_cache


from kafka import KafkaProducer

#######################################################################################
# ENVIRONMENT CONFIGS

KAFKA_PASS=os.getenv('KAFKA_SASL_PASS')
KAFKA_USER=os.getenv('KAFKA_SASL_USER')
KAFKA_BRKR=os.getenv('KAFKA_HOST')
KAFKA_PORT=os.getenv('KAFKA_PORT')
KAFKA_TOPIC=os.getenv('KAFKA_TOPIC')

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
	key_serializer=str.encode,
	value_serializer=lambda x:
	json.dumps(x).encode('utf-8'))
logging.info('Connecting to Kafka, completed')

def stream(from_cache=True, from_exchange=False):
	
	if from_exchange:
		logging.info("Pulling quotes from exchange")
		#all_quotes = pull_cache()
		all_quotes = pull_quotes()
	if from_cache:
		logging.info("Pulling quotes from cache")
		all_quotes = pull_cache()
	
	px_now = {}
	for s in all_quotes:
		if "quote" in all_quotes[s]:
			if "latestPrice" in all_quotes[s]["quote"]:
				if all_quotes[s]["quote"]["latestPrice"]:
					px_now[s] = all_quotes[s]["quote"]["latestPrice"]
	
	while True:
		
		time.sleep(TIME_BETWEEN_LOOPS)

		push_stamp = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") #.isoformat()
		for c in px_now.keys():
			#print(f"Handling ticker: {c} with price {px_now[c]}")
			newprice = px_now[c] + (random.random()-0.5)/2
			px_now[c] = round(newprice, 2)
			msg = {
				"symbol": c,
				"px_last": px_now[c],
				"px_timestamp": push_stamp,
				"size": int(random.random() * 10000)
			}
			producer.send(KAFKA_TOPIC, value=msg, key=c)
		producer.flush()

		print(f"{push_stamp}   Tick!  Disney: {px_now['DIS']} \t AIG: {px_now['AIG']} \t Goldman Sachs: {px_now['GS']} \t Accenture: {px_now['ACN']}")
		
if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument('--from-cache', action='store_true')
	parser.add_argument('--from-exchange', action='store_true')
	argx = parser.parse_args()
	if (argx.from_cache and argx.from_exchange):
		logging.error("Cannot operate both from-exchange and from-cache simultaneously")
		sys.exit()
	stream(from_cache=argx.from_cache, from_exchange=argx.from_exchange)