#!/bin/bash

# Make sure to update the build.gradle to point JAR to JobOneDriver

rm -rf ~/csx55/hw3/maxsongs_final
hadoop fs -rm -r /hw3/maxsongs
hadoop fs -rm -r /hw3/maxsongs_final

hadoop jar ~/csx55/hw3/build/libs/hw3-1.0-SNAPSHOT.jar JobOneDriver /hw3/metadata.txt /hw3/maxsongs
hadoop fs -get /hw3/maxsongs_final
