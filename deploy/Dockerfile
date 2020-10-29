ARG BASE_OS=alpine3.12
FROM golang:1.15.3-${BASE_OS} as build
WORKDIR /flink-deployer
COPY . .
RUN go mod download
RUN go build ./cmd/cli

FROM golang:1.15.3-${BASE_OS}
COPY --from=build /flink-deployer/cli /bin/flink-deployer
CMD [ "flink-deployer" ]
