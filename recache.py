import datetime
import time
import io
import json
import logging

CACHE_FILE = "cache.csv"

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

def pull_quotes():
	import pyEX as p

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

	dt = datetime.datetime.now() 
	dt_string = dt.strftime("%Y_%m_%d__%H_%M_%S")
	ts_filename = f"quotes_cache___{dt_string}.json"

	results_merged = {}
	for r in results:
		for k in r.keys():
			results_merged[k]=r[k]

	push_cache(results_merged, cache_file=ts_filename)

	return results_merged

def pull_cache(cache_file=CACHE_FILE):
	logging.info(f'Pulling local price cache from {cache_file}')
	with open(cache_file, "r") as read_file:
	    cache = json.load(read_file)
	return cache

def push_cache(quotes_cache, cache_file=CACHE_FILE):
	logging.info(f'Pushing local price cache to {cache_file}')
	with open(CACHE_FILE,'w') as f:    
		json.dump(quotes_cache, f, indent=4)

def main():

	try:
		fresh_quotes = pull_quotes()
		push_cache(fresh_quotes)
		logging.info(f"Found {len(fresh_quotes)} tickers to pull")
	except:
		print("Problems")
		
if __name__ == "__main__":
	main()