import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LoudestAverageSongs {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{
        private Text artist_id = new Text();
        private DoubleWritable loudnessScore = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\\|");

            artist_id.set(items[34]);
            loudnessScore.set(Double.parseDouble(items[9]));

            context.write(artist_id, loudnessScore);
        }
    }

    public static class AverageReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int totalCount = 0;
            int sumOfTerms = 0;

            for (DoubleWritable value : values) {
                sumOfTerms += value.get();
                totalCount++;
            }

            double average = (double) sumOfTerms / totalCount;
            result.set(average);
            context.write(key, result);
        }
    }


    /*
        Input: <artist_id, total ct of songs>
        Output: <artist with max songs, # that is max songs>
    */
    public static class MaxDoubleReducer extends Reducer<Text,Text,Text,DoubleWritable> {
        private Text max_artist = new Text();
        private DoubleWritable max_avg_loudness = new DoubleWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double max = -1;
            for (Text value : values) {
                String[] items = value.toString().split("\\s+");

                String artist_id = items[0];
                double avg_loudness = Double.parseDouble(items[1]);

                if (avg_loudness > max) {
                    max_artist.set(artist_id);
                    max_avg_loudness.set(avg_loudness);
                    max = avg_loudness;
                }
            }
            context.write(max_artist, max_avg_loudness);
        }
    }
}