FROM golang:1.14.4-alpine3.12 as build
WORKDIR /flink-deployer
COPY . .
RUN go mod download
RUN go build ./cmd/cli

FROM alpine:3.12
WORKDIR /flink-deployer
COPY --from=build /flink-deployer/cli .
VOLUME [ "/data/flink" ]
ENTRYPOINT [ "/flink-deployer/cli" ]
CMD [ "help" ]
