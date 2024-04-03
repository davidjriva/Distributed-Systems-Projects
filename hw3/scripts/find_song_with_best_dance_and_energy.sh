#!/bin/bash

rm -rf ~/csx55/hw3/bestEnergyAndDance_final
hadoop fs -rm -r /hw3/bestEnergyAndDance_final

hadoop jar ~/csx55/hw3/build/libs/hw3-1.0-SNAPSHOT.jar MostEnergyAndDance /hw3/analysis.txt /hw3/bestEnergyAndDance_final
hadoop fs -get /hw3/bestEnergyAndDance_final