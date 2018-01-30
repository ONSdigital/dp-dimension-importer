dp-dimension-importer
================

### Getting started
TODO

### Configuration

| Environment variable                | Default                                | Description
| ----------------------------------- | -------------------------------------- | -----------
| BIND_ADDR                           | ":21000"                               | The host and port to bind to
| KAFKA_ADDR                          | "localhost:9092"                       | The list of kafka hosts
| DATASET_API_ADDR                    | "http://localhost:21800"               | The address of the dataset API
| DATASET_API_AUTH_TOKEN              | "FD0108EA-825D-411C-9B1D-41EF7727F465" | The authentication token for the dataset API
| DB_URL                              | "bolt://localhost:7687"                | The URL of the database
| DB_POOL_SIZE                        | "20"                                   | The number of database connections to maintain in a pool
| DIMENSIONS_EXTRACTED_TOPIC          | "dimensions-extracted"                 | The topic to consume messages from when dimensions are extracted
| DIMENSIONS_EXTRACTED_CONSUMER_GROUP | "dp-dimension-importer"                | The consumer group to consume messages from when dimensions are extracted
| DIMENSIONS_INSERTED_TOPIC           | "dimensions-inserted"                  | The topic to write output messages when dimensions are inserted
| EVENT_REPORTER_TOPIC                | "report-events"                        | The topic to write output messages when any errors occur during processing an instance
| GRACEFUL_SHUTDOWN_TIMEOUT           | "5s"                                   | The graceful shutdown timeout in seconds
| HEALTHCHECK_INTERVAL                | "60s"                                  | How often to run a health check

### Healthcheck

 The `/healthcheck` endpoint returns the current status of the service. Dependent services are health checked on an interval defined by the `HEALTHCHECK_INTERVAL` environment variable.

 On a development machine a request to the health check endpoint can be made by:

 `curl localhost:22500/healthcheck`

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2017, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

