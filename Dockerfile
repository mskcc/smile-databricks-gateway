# syntax=docker/dockerfile:1

# official go image has all necessary tools and libs to compile/run go app
FROM golang:1.24.0 AS build-stage

ENV GOPRIVATE=github.mskcc.org/*

ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

RUN git config --global url."https://${GITHUB_TOKEN}@github.mskcc.org/".insteadOf "https://github.mskcc.org/"

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
FROM golang:1.24.0 AS build-release-stage

WORKDIR /

COPY --from=build-stage /sdg /smile-databricks-gateway

# execute the gateway when container starts
CMD ["./smile-databricks-gateway"]
