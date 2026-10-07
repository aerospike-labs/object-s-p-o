#!/bin/bash
#


#go run ./cmd/queryload/ -hosts ... -duration 60s -workers 16
./queryload --hosts 172.31.13.162 --duration 300s --workers 16 --qps 0
