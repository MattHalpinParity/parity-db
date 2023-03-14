#!/usr/bin/env bash

# Run stress test multiple times to collect data

#RUN_DATA_NAME=Default
RUN_DATA_NAME=$(git rev-parse --abbrev-ref HEAD)

RUN_WITHOUT_ARCHIVE=true
RUN_WITH_ARCHIVE=true

NUM_SAMPLES=1

echo Dataset name: ${RUN_DATA_NAME}

for c in 10000 15000 20000 40000 60000 80000 100000 125000 150000 175000 200000;
do
	echo Executing with ${c} commits
	for ((i=0; i<$NUM_SAMPLES; i++));
	do
		if [[ "$RUN_WITHOUT_ARCHIVE" = true ]]; then
			echo Executing sample ${i} without archive
			echo 
			cargo run --release -p parity-db-admin -- stress --commits ${c} --uniform --run-data-name ${RUN_DATA_NAME}
			echo 
		fi
		if [[ "$RUN_WITH_ARCHIVE" = true ]]; then
			echo Executing sample ${i} with archive
			echo 
			cargo run --release -p parity-db-admin -- stress --commits ${c} --uniform --archive --run-data-name ${RUN_DATA_NAME}
			echo 
		fi
	done
done