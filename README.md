# chs-streaming-api-backend

## Contents

The Companies House Streaming Platform Backend consumes offsets from streaming API topics on Kafka, serialises these in entities containing offset data and the offset number, and pushes these to connected users as an event stream.

## Requirements

The following services and applications are required to build and/or run chs-streaming-api-backend:

* AWS ECR
* Docker
* Apache Kafka
* Kafka schema registry

You will need an HTTP client that supports server-sent events (e.g. cURL) to connect to the service and receive published offsets.

## Building and Running Locally

1. Login to AWS ECR.
2. Build the project by running `docker build` from the project directory.
3. Run the Docker image that has been built by running `docker run IMAGE_ID` from the command line, ensuring values have been specified for the KAFKA_STREAMING_BROKER_ADDR and SCHEMA_REGISTRY_URL environment variables and that port 6000 is exposed.
4. Send a GET request using your HTTP client to /filings. A connection should be established and any offsets published to the stream-filing-history topic should appear in the response body.

## Configuration

Variable|Description|Example|Mandatory|
--------|-----------|-------|---------|
KAFKA_STREAMING_BROKER_ADDR|The address of the Kafka broker|chs-kafka:9092|yes
SCHEMA_REGISTRY_URL|The URL of the Kafka schema registry|http://chs-kafka:8081|yes
BIND_ADDRESS|The port that will be opened to allow incoming connections (default 6000)|:8080|no
CERT_FILE| |/path/to/cert/file|no
KEY_FILE| |/path/to/key/file|no
