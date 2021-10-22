#!/bin/bash

# 
# This assumes the following environment variables
# 
# 
# Destination Kafka Queue:
#   Environment Variable: KAFKA_SASL_PASS
#   Environment Variable: KAFKA_SASL_USER
#   Environment Variable: KAFKA_HOST
#   Environment Variable: KAFKA_PORT
#   Environment Variable: KAFKA_TOPIC
#
# Exchange feed tocken (if running in MARKET_MODE)
#   Obtain one from https://intercom.help/iexcloud/en/
#   Environment Variable: IEX_TOKEN
#
# Execution Mode (cache vs straight-from-exchange)
#   Environment Variable: EXECUTION_MODE = MARKET_MODE | CACHE_MODE
#

# -------------------------------------
# Run in Market Mode
if $EXECUTION_MODE = "MARKET_MODE"
then
    python stream.py --from-exchange
fi
# -------------------------------------


# -------------------------------------
# Run in Cache Mode
if $EXECUTION_MODE = "CACHE_MODE"
then
    python stream.py --from-cache
fi
# -------------------------------------