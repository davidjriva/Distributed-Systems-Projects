#!/bin/bash

# Remove output file if it already exists
rm -rf ~/csx55/hw3/highestFading_final
hadoop fs -rm -r /hw3/highestFading_final

# Run hadoop job
hadoop jar ~/csx55/hw3/build/libs/hw3-1.0-SNAPSHOT.jar HighestFadingTime /hw3/combined.txt /hw3/highestFading_final
hadoop fs -get /hw3/highestFading_final