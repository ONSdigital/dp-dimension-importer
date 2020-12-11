dp-dimension-importer
================

Handles inserting of dimensions into database after input file becomes available;
and creates an event by sending a message to a dimension-imported kafka topic so further processing of the input file can take place.

Requirements
------------------
In order to run the service locally you will need the following:
- [Go](https://golang.org/doc/install)
- [Git](https://git-scm.com/downloads)
- [Kafka](https://kafka.apache.org/)
- [Dataset API](https://github.com/ONSdigital/dp-dataset-api)
- [API AUTH STUB](https://github.com/ONSdigital/dp-auth-api-stub)

### Getting started

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
| KAFKA_ADDR                          | localhost:9092                       | The list of kafka hosts
| DATASET_API_ADDR                    | http://localhost:21800               | The address of the dataset API
| DATASET_API_AUTH_TOKEN              | FD0108EA-825D-411C-9B1D-41EF7727F465 | The authentication token for the dataset API
| DIMENSIONS_EXTRACTED_TOPIC          | dimensions-extracted                 | The topic to consume messages from when dimensions are extracted
| DIMENSIONS_EXTRACTED_CONSUMER_GROUP | dp-dimension-importer                | The consumer group to consume messages from when dimensions are extracted
| DIMENSIONS_INSERTED_TOPIC           | dimensions-inserted                  | The topic to write output messages when dimensions are inserted
| EVENT_REPORTER_TOPIC                | report-events                        | The topic to write output messages when any errors occur during processing an instance
| GRACEFUL_SHUTDOWN_TIMEOUT           | 5s                                   | The graceful shutdown timeout (time.Duration)
| HEALTHCHECK_INTERVAL                | 30s                                  | The period of time between health checks (time.Duration)
| HEALTHCHECK_CRITICAL_TIMEOUT        | 90s                                  | The period of time after which failing checks will result in critical global check (time.Duration)
| SERVICE_AUTH_TOKEN                  | 4424A9F2-B903-40F4-85F1-240107D1AFAF | The service authorization token
| ZEBEDEE_URL                         | http://localhost:8082                | The host name for Zebedee

### Healthcheck

 The `/healthcheck` endpoint returns the current status of the service. Dependent services are health checked on an interval defined by the `HEALTHCHECK_INTERVAL` environment variable.

 On a development machine a request to the health check endpoint can be made by:

 `curl localhost:23000/healthcheck`

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License


Copyright © 2016-2017, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
