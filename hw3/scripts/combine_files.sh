#!/bin/bash

rm -rf ~/csx55/hw3/combined_final
hadoop fs -rm -r /hw3/combined_final

hadoop jar ~/csx55/hw3/build/libs/hw3-1.0-SNAPSHOT.jar TextFileJoin