#!/bin/bash


if [ -e bundle.client.tar ]; then rm bundle.client.tar; fi

go build -o generate ./cmd/generate
go build -o queryload ./cmd/queryload
go build -o load ./cmd/load
GOOS=linux GOARCH=amd64 go build -o generate.linux ./cmd/generate
GOOS=linux GOARCH=amd64 go build -o queryload.linux ./cmd/queryload
GOOS=linux GOARCH=amd64 go build -o load.linux ./cmd/load

tar -c --no-xattrs --no-mac-metadata -f bundle.client.tar generate.linux queryload.linux load.linux descriptors/* model/* store/* graph/*
