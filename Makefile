root_dir:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

SRC = $(shell find . -name '*.go')
SRC_TEST = $(shell find . -name '*_test.go')

VERSION = $(shell git tag)
ifeq ($(strip $(VERSION)),)
VERSION=unknown
endif
GIT_COMMIT = $(shell git describe --always --long)


pgCarpenter: $(SRC)
	go build -ldflags=all="-X main.version=$(VERSION) -X main.gitCommit=$(GIT_COMMIT)"

.PHONY: fmt
fmt: $(SRC)
	$(foreach file, $^, go fmt $(file);)

.PHONY: test
test: $(SRC_TEST)
	go test -coverprofile=coverage.out ./...

.PHONY: coverage
coverage: test
	go tool cover -html=coverage.out
