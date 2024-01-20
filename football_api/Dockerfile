# syntax=docker/dockerfile:1

FROM golang:1.21.5

#destination for copy
WORKDIR /app

#Download Go Modules
COPY go.mod go.sum ./
RUN go mod download

#Copy source code.
COPY *.go ./

#build
RUN go build -o /go-api

EXPOSE 8080

CMD ["/go-api"]