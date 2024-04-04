#!/bin/bash

rm -rf ~/csx55/hw3/mostgeneric_final
hadoop fs -rm -r /hw3/mostgeneric_final

hadoop jar ~/csx55/hw3/build/libs/hw3-1.0-SNAPSHOT.jar MostGenericArtist /hw3/metadata.txt /hw3/mostgeneric_final
hadoop fs -get /hw3/mostgeneric_final

