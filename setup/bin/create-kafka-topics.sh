#!/bin/bash

# Determine location of this script.
dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Print usage.
printUsage() {
  echo "Usage: $0 <zookeeper quorum>"
  echo
  echo "e.g. $0 localhost:2181 or '10.0.26.52:2181'"
}

# Check args.
if [[ $# -ne 1 ]]; then
  printUsage
  exit 1
fi

zk=$1


## Create Kafka topics

# Data ingest topic (inbound from Kapua)
kafka-topics --zookeeper $zk --create --topic ingest --partitions 1 --replication-factor 1

# Model topic (outbound from Spark model training)
kafka-topics --zookeeper $zk --create --topic model --partitions 1 --replication-factor 1



