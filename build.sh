#!/bin/bash


if [ -e bundle.client.tar ]; then rm bundle.client.tar; fi

go build -o generate ./cmd/generate
go build -o queryload ./cmd/queryload
go build -o load ./cmd/load
go build -o queryapi ./cmd/queryapi
GOOS=linux GOARCH=amd64 go build -o generate.linux ./cmd/generate
GOOS=linux GOARCH=amd64 go build -o queryload.linux ./cmd/queryload
GOOS=linux GOARCH=amd64 go build -o load.linux ./cmd/load
GOOS=linux GOARCH=amd64 go build -o queryapi.linux ./cmd/queryapi

tar -c --no-xattrs --no-mac-metadata -f bundle.client.tar generate.linux queryload.linux load.linux queryapi.linux deploy/* descriptors/* model/* store/* graph/*
