# syntax=docker/dockerfile:1

# official go image has all necessary tools and libs to compile/run go app
FROM golang:alpine AS build-stage

# set destination for copy commands
WORKDIR /smile-databricks-gateway

# download go modules
COPY go.mod go.sum ./
RUN go mod download

# copy source code
COPY cmd ./cmd/
COPY *.go .

# build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 cd ./cmd/cli && go build -o /sdg

# Deploy the application binary into a lean image
FROM golang:alpine AS build-release-stage

WORKDIR /

COPY --from=build-stage /sdg /smile-databricks-gateway

# execute the gateway when container starts
CMD ["./smile-databricks-gateway"]
