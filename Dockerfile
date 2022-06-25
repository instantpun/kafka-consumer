# syntax=docker/dockerfile:1

##
## Build
##
FROM golang:1.18.1-buster AS build

WORKDIR /go/src/app
COPY *.go /go/src/app/

RUN go mod init && \
    go mod tidy && \
    echo $GOPATH
RUN go get -d -v ./...
RUN go vet -v
# RUN go test -v
RUN CGO_ENABLED=1 go build -o /go/bin/go-consumer

##
## Deploy
##
FROM gcr.io/distroless/base-debian10:debug

WORKDIR /

COPY --from=build /go/bin/go-consumer /

EXPOSE 28080

USER nonroot:nonroot

ENTRYPOINT ["/go-consumer"]

