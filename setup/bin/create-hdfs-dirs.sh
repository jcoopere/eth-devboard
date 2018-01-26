#!/bin/bash

# Determine location of this script.
dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Print usage.
printUsage() {
  echo "Usage: $0"
  echo
  echo "e.g. $0"
}

# Check args.
if [[ $# -ne 0 ]]; then
  printUsage
  exit 0
fi


## Create HDFS dir for model output.
sudo -u hdfs hadoop fs -mkdir /model

## Open permissions for model dir.
sudo -u hdfs hadoop fs -chmod -R 777 /model

## Add training data to HDFS.
sudo -u hdfs hadoop fs -put training-data /training-data
sudo -u hdfs hadoop fs -chmod -R 777 /training-data



