#!/bin/bash

rows=100,000,000

rows=$(echo ${rows} | sed 's/,//g')

./generate --host 172.31.13.162 --workers 32 --count ${rows} --hash-type sha-512 --batch 1000

