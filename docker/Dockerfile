# The Go build runs on the build host and cross-compiles, so multi-arch images
# need no emulation.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build-stage

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags "-s -w -X main.version=${VERSION}" -o bin/firecrest-bridge ./cmd/firecrest-bridge

FROM alpine:3.22

# kinit for firecrest-stage, which takes a Kerberos ticket for a volume that needs one
RUN apk add --no-cache krb5

COPY --from=build-stage /app/bin/firecrest-bridge /usr/local/bin/firecrest-bridge
RUN ln -s firecrest-bridge /usr/local/bin/firecrest-stage

ENV FIRECRESTCONFIGPATH=/etc/interlink/FirecrestConfig.yaml

ENTRYPOINT ["/usr/local/bin/firecrest-bridge"]
CMD ["daemon"]
