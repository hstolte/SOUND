#!/usr/bin/env bash

set -e
set -x

# Copy Flink configuration
cp "configs/flink/flink-conf.yaml" flink-1.14.0/conf/

