#!/bin/bash

# Remove output file if it already exists
rm -rf ~/csx55/hw3/loudestAvgSongs_final

hadoop fs -rm -r /hw3/loudestAvgSongs
hadoop fs -rm -r /hw3/loudestAvgSongs_final

# Run hadoop job
hadoop jar ~/csx55/hw3/build/libs/hw3-1.0-SNAPSHOT.jar JobTwoDriver /hw3/combined.txt /hw3/loudestAvgSongs
hadoop fs -get /hw3/loudestAvgSongs_final