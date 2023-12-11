#!/bin/bash

# Bash script to generate random data and publish it to a Kafka topic using kafkacat

# Usage: ./publish_data.sh <bootstrap_servers> <topic> <num_records>

bootstrap_servers=$1
topic=$2
num_records=$3

# Generate random data
generate_data() {
  i=0
  while [ $i -lt $num_records ]; do
    data=$(printf "%d,%d,%d,%d\n" $RANDOM $RANDOM $RANDOM $RANDOM)
    echo $data
    i=$((i + 1))
  done
}

# Check if topic exists, create it if not
check_create_topic() {
  topic_exists=$(kcat -L -b "$bootstrap_servers" -t "$topic" 2>/dev/null | grep -c "$topic")
  if [ $topic_exists -eq 0 ]; then
    echo "Topic '$topic' does not exist. Creating..."
    kcat -b "$bootstrap_servers" -t "$topic" -p 1 -c 1 -e 2>/dev/null
    sleep 2
  fi
}

# Publish data to Kafka
publish_data() {
  generate_data | kcat -P -b "$bootstrap_servers" -t "$topic"
}

# Main script
if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <bootstrap_servers> <topic> <num_records>"
  exit 1
fi

check_create_topic
publish_data
