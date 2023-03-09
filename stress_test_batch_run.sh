#!/usr/bin/env bash

# Run stress test multiple times to collect data

RUN_DATA_NAME=Default

NUM_SAMPLES=3

echo Dataset name: ${RUN_DATA_NAME}

for c in 10000 15000 20000 40000 60000 100000;
do
	echo Executing with ${c} commits
	for ((i=0; i<$NUM_SAMPLES; i++));
	do
		echo Executing sample ${i} with pruning
		echo 
		cargo run --release -p parity-db-admin -- stress --commits ${c} --uniform --run-data-name ${RUN_DATA_NAME}
		echo 
		echo Executing sample ${i} without pruning
		echo 
		cargo run --release -p parity-db-admin -- stress --commits ${c} --uniform --archive --run-data-name ${RUN_DATA_NAME}
		echo 
	done
done