import datetime
import time
import io
import json
import logging

import pyEX as p

CACHE_FILE = "cache.csv"

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

def pull_quotes():
	logging.info('Connecting to IEX for price cache')
	c = p.Client()
	tickers_all = p.symbolsList()
	tickers_good = []
	for t in tickers_all:
		if '+' in t or '-' in t or '=' in t:
			pass
		else:
			tickers_good.append(t)

	# chunk out, since we cant 
	tickers_good_chunks = [tickers_good[i:i+100] for i in range(0, len(tickers_good), 100)]

	logging.info(f"Found {len(tickers_good)} tickers to pull")

	results = []
	for ticker_chunk in tickers_good_chunks:
		print(f"Pulling prices for {ticker_chunk}")
		ret = c.batch(
			ticker_chunk,
			fields=["quote"],
			format="json"
		)
		results.append(ret)


	results_merged = {}
	for r in results:
		for k in r.keys():
			results_merged[k]=r[k]

	return results_merged

def main():

	try:

		results_merged = pull_quotes()

		with open(CACHE_FILE,'w') as twitterDataFile:    
			json.dump(results_merged, twitterDataFile, indent=4)

		logging.info(f"Found {len(results_merged)} tickers to pull")

	except:
		print("Problems")
		
if __name__ == "__main__":
	main()