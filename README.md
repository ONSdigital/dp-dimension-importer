dp-dimension-importer
================

### Getting started

### Configuration

| Environment variable       | Default                              | Description
| -------------------------- | ------------------------------------ | -----------
| BIND_ADDR                  | :21000                               | The host and port to bind to
| KAFKA_ADDR                 | localhost:9092                       | The list of kafka hosts
| IMPORT_ADDR                | http://localhost:21800               | The address of the import API
| IMPORT_AUTH_TOKEN          | FD0108EA-825D-411C-9B1D-41EF7727F465 | The authentication token for the import API
| DB_URL                     | bolt://localhost:7687                | The URL of the database
| DB_POOL_SIZE               | 20                                   | The number of database connections to maintain in a pool
| DIMENSIONS_EXTRACTED_TOPIC | dimensions-extracted                 | The topic to consume messages from to when dimensions are extracted
| DIMENSIONS_INSERTED_TOPIC  | dimensions-inserted                  | The topic to write output messages when dimensions are inserted

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2017, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

