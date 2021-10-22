FROM python:3.7-slim

LABEL maintainer="support@kinetica.com"
LABEL Description="Kinetica Machine Learning 'Bring Your Own Container' Sample Continuous Ingestor: US Equities Quotes Producer"
LABEL Author="Saif Ahmed"

RUN mkdir /opt/gpudb
RUN mkdir /opt/gpudb/kml
RUN mkdir /opt/gpudb/kml/slipway
WORKDIR /opt/gpudb/kml/slipway

COPY requirements.txt ./

RUN pip install -r requirements.txt --no-cache-dir

COPY stream.py ./
COPY recache.py ./
COPY quote_cache.json ./
COPY runner.sh ./

CMD ["/opt/gpudb/kml/slipway/runner.sh"]
