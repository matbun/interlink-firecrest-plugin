VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE ?= firecrest-bridge:dev

.PHONY: all build test vet render image integration integration-down

all: build

build:
	CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=$(VERSION)" -o bin/firecrest-bridge ./cmd/firecrest-bridge

test:
	go test -race ./...

vet:
	go vet ./...

# Render the interLink chart with the values files in deploy/ and test/e2e/.
render:
	test/render/run.sh

image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) -f docker/Dockerfile .

# Local end-to-end test against FirecREST's demo stack; see test/integration/README.md.
integration: image
	test/integration/run.sh

integration-down:
	test/integration/run.sh down
