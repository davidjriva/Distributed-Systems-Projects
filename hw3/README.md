Starting up hdfs: '$HADOOP_HOME/sbin/start-dfs.sh'

Master Namnode: jackson
Second Namenode: jefferson-city
Workers: juneau lansing lincoln little-rock madison montgomery montepelier nashville oklahoma-city olympia
Port Range: 31370-31384

Spark and Hadoop Configurations are now complete. Please check and make sure that the courses/cs535/pa1 module is loaded with the "module list" command.

Start/stop hdfs with the 
"$HADOOP_HOME/sbin/start-dfs.sh"
"$HADOOP_HOME/sbin/stop-dfs.sh"

Start/stop yarn with the 
"$HADOOP_HOME/sbin/start-yarn.sh"
"$HADOOP_HOME/sbin/stop-yarn.sh"

Start/stop spark with the 
"start-all.sh"
"stop-all.sh" 

JOBS:
Q1. Which artist has the most songs in the data set? 
    File -- JobOneDriver.java
    Run script -- ~/csx55/hw3/scripts/find_max_song_in_dataset.sh
    
Q2. Which artist's songs are the loudest on average?
    File -- JobTwoDriver.java
    Run Script -- ~/csx55/hw3/scripts/find_artist_with_loudest_avg_songs.sh

Q3. What is the song with the highest hotttness (popularity) score?
    File -- HottestSongsInDataset.java
    Run Script -- ~/csx55/hw3/scripts/find_hottest_songs.sh

Q4. Which artist has the highest total time spent fading in their songs?
    File -- HighestFadingTime.java
    Run Script -- /s/bach/l/under/driva/csx55/hw3/scripts/find_artist_with_highest_fading_time.sh