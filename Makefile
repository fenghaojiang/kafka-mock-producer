.PHONY: build 


run:
	go run . kafka-mock-producer --topic ethereum_logs --broker-list localhost:9094 --interval 1 


build: 
	mkdir -p bin
	go build -o bin/kafka-mock-producer . 