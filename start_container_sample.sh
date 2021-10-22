#! /bin/bash

repo_uri=$(cat repo_uri.info)

docker run -it \
	-e KAFKA_HOST=pkc-187.us-east-2.aws.confluent.cloud \
	-e KAFKA_PORT=9092 \
	-e KAFKA_SASL_USER=SD6 \
	-e KAFKA_SASL_PASS=PROFIT5 \
	-e KAFKA_TOPIC_STATION_STATUS=nyc-citibike-station-status \
	-e KAFKA_TOPIC_STATION_INFORM=nyc-citibike-station-info \
	-e REFRESH_DELAY_SECONDS=60 \
	$repo_uri
