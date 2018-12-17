root_dir:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

SRC = $(shell find . -name '*.go')
SRC_TEST = $(shell find . -name '*_test.go')

callme: $(SRC)
	go build

.PHONY: fmt
fmt: $(SRC)
	$(foreach file, $^, go fmt $(file);)

.PHONY: test
test: $(SRC_TEST)
	go test -coverprofile=coverage.out ./...

.PHONY: coverage
coverage: test
	go tool cover -html=coverage.out
