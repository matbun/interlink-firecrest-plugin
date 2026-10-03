FROM golang:1.26 AS build-stage

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w -X main.version=${VERSION}" -o bin/firecrest-bridge ./cmd/firecrest-bridge

FROM alpine:3.22

COPY --from=build-stage /app/bin/firecrest-bridge /usr/local/bin/firecrest-bridge

ENV FIRECRESTCONFIGPATH=/etc/interlink/FirecrestConfig.yaml

ENTRYPOINT ["/usr/local/bin/firecrest-bridge"]
CMD ["daemon"]
