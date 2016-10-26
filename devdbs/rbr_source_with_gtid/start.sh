#!/bin/bash

set -eu

# Convert 48bit hostname hash to a 32bit int
# also skip name resolution because it causes significant delays in connecting
# to the server
mysqld --server-id=$((0x$(hostname) >> 16))
