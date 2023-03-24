#!/usr/bin/env bash

# Run stress test multiple times to collect data

#RUN_DATA_NAME=Default
RUN_DATA_NAME=$(git rev-parse --abbrev-ref HEAD)

RUN_FOR_COMMITS=true
RUN_FOR_QUERIES=true

NUM_SAMPLES=3

NUM_COMMITS=200000

echo Dataset name: ${RUN_DATA_NAME}

for ((i=0; i<$NUM_SAMPLES; i++));
do
	if [[ "$RUN_FOR_COMMITS" = true ]]; then
		echo Executing sample ${i} for commits
		echo 
		cargo run --release -p parity-db-admin -- stress --commits ${NUM_COMMITS} --uniform --commits-per-timing-sample 10000 --run-data-name ${RUN_DATA_NAME}
		echo 
	fi
	if [[ "$RUN_FOR_QUERIES" = true ]]; then
		echo Executing sample ${i} for queries
		echo 
		cargo run --release -p parity-db-admin -- stress --commits ${NUM_COMMITS} --uniform --readers 4 --writer-commits-per-sleep 100 --writer-sleep-time 100 --commits-per-timing-sample 10000 --run-data-name ${RUN_DATA_NAME}
		echo 
	fi
done