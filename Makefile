build:
	go build -o build/dp-dimension-importer

debug: build
	HUMAN_LOG=1 ./build/dp-dimension-importer -bind-addr=":20000" -import-addr="http://localhost:21000"

.PHONY: build debug