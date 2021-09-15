# dp-dimension-importer

Handles inserting of dimensions into database after input file becomes available;
and creates an event by sending a message to a dimension-imported kafka topic so further processing of the input file can take place.

## Requirements

In order to run the service locally you will need the following:
- [Go](https://golang.org/doc/install)
- [Git](https://git-scm.com/downloads)
- [Kafka](https://kafka.apache.org/)
- [Dataset API](https://github.com/ONSdigital/dp-dataset-api)
- [API AUTH STUB](https://github.com/ONSdigital/dp-auth-api-stub)

## Getting started

* Clone the repo `go get github.com/ONSdigital/dp-dimension-importer`
* Run kafka and zookeeper
* Run local S3 store
* Run the dataset API, see documentation [here](https://github.com/ONSdigital/dp-dataset-api)
* Run api auth stub, see documentation [here](https://github.com/ONSdigital/dp-auth-api-stub)
* Run the application with `make debug`

### Kafka scripts

Scripts for updating and debugging Kafka can be found [here](https://github.com/ONSdigital/dp-data-tools)(dp-data-tools)

### Configuration

| Environment variable                | Default                              | Description
| ----------------------------------- | ------------------------------------ | -----------
| BIND_ADDR                           | :23000                               | The host and port to bind to
| SERVICE_AUTH_TOKEN                  | 4424A9F2-B903-40F4-85F1-240107D1AFAF | The service authorization token
| KAFKA_ADDR                          | localhost:9092                       | The list of kafka hosts
| BATCH_SIZE                          | 1                                    | Number of kafka messages that will be batched
| KAFKA_NUM_WORKERS                   | 1                                    | The maximum number of concurent kafka messages being consumed at the same time
| KAFKA_VERSION                       | "1.0.2"                              | The kafka version that this service expects to connect to
| KAFKA_OFFSET_OLDEST                 | true                                 | sets the kafka offset to be oldest if true 
| KAFKA_SEC_PROTO                     | _unset_                              | if set to `TLS`, kafka connections will use TLS [[1]](#notes_1)
| KAFKA_SEC_CLIENT_KEY                | _unset_                              | PEM for the client key [[1]](#notes_1)
| KAFKA_SEC_CLIENT_CERT               | _unset_                              | PEM for the client certificate [[1]](#notes_1)
| KAFKA_SEC_CA_CERTS                  | _unset_                              | CA cert chain for the server cert [[1]](#notes_1)
| KAFKA_SEC_SKIP_VERIFY               | false                                | ignores server certificate issues if `true` [[1]](#notes_1)
| DATASET_API_ADDR                    | http://localhost:21800               | The address of the dataset API
| DIMENSIONS_EXTRACTED_TOPIC          | dimensions-extracted                 | The topic to consume messages from when dimensions are extracted
| DIMENSIONS_EXTRACTED_CONSUMER_GROUP | dp-dimension-importer                | The consumer group to consume messages from when dimensions are extracted
| DIMENSIONS_INSERTED_TOPIC           | dimensions-inserted                  | The topic to write output messages when dimensions are inserted
| EVENT_REPORTER_TOPIC                | report-events                        | The topic to write output messages when any errors occur during processing an instance
| GRACEFUL_SHUTDOWN_TIMEOUT           | 5s                                   | The graceful shutdown timeout (time.Duration)
| HEALTHCHECK_INTERVAL                | 30s                                  | The period of time between health checks (time.Duration)
| HEALTHCHECK_CRITICAL_TIMEOUT        | 90s                                  | The period of time after which failing checks will result in critical global check (time.Duration)
| ENABLE_PATCH_NODE_ID                | true                                 | If true, the NodeID value for a dimension option stored in Neptune will be sent to dataset API

**Notes:**

1. <a name="notes_1">For more info, see the [kafka TLS examples documentation](https://github.com/ONSdigital/dp-kafka/tree/main/examples#tls)</a>

### Healthcheck

 The `/healthcheck` endpoint returns the current status of the service. Dependent services are health checked on an interval defined by the `HEALTHCHECK_INTERVAL` environment variable.

 On a development machine a request to the health check endpoint can be made by:

 `curl localhost:23000/healthcheck`

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2021, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.