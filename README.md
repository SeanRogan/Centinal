# Crypto Market Data Streaming Service

A real-time data pipeline that streams cryptocurrency exchange market data into TimescaleDB using Kafka Streams for efficient processing and analysis.

## Overview

This service captures live market data from cryptocurrency exchanges, processes it through Kafka Streams, and stores it in a TimescaleDB instance for time-series analysis and querying. 

## Architecture

Source API -> Centinal Routing/Kafka Source -> Timeseries DB/Kafka Sink

## Env Requirements
Several environmental variables are required to deploy the application:
COINBASE_API_KEY: your coinbase api key
COINBASE_API_SECRET: your coinbase secret
COINBASE_API_PASSPHRASE: your coinbase passphrase
SPRING_DATASOURCE_URL: your timeseries db connection url
SPRING_DATASOURCE_USERNAME: your timeseries db username
SPRING_DATASOURCE_PASSWORD: your timeseries db password
