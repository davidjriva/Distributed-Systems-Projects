#!/bin/bash

# rm -rf ~/csx55/hw3/yearcount_final
# rm -rf ~/csx55/hw3/avgHottness_final
# rm -rf ~/csx55/hw3/percentChange_final
rm -rf ~/csx55/hw3/maxPercent_final

# hadoop fs -rm -r /hw3/yearcount_final
# hadoop fs -rm -r /hw3/avgHottness_final
# hadoop fs -rm -r /hw3/percentChange_final
hadoop fs -rm -r /hw3/maxPercent_final

hadoop jar ~/csx55/hw3/build/libs/hw3-1.0-SNAPSHOT.jar CustomDriver /hw3/combined.txt /hw3/yearcount_final

# hadoop fs -get /hw3/yearcount_final
# hadoop fs -get /hw3/avgHottness_final
# hadoop fs -get /hw3/percentChange_final
hadoop fs -get /hw3/maxPercent_final
