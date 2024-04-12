import java.io.IOException;
import java.util.StringTokenizer;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.WritableComparator;

public class MaxSongsInDataset {
    /*
        Input: a line of text delimited by |
        Output: <artist_id, 1>
    */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text artist_id = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Read in a line and split based on delimiter
            String line = value.toString();
            String[] items = line.split("\\|");

            // Output <artist_id, 1> to be summed at the reducer
            artist_id.set(items[2]);

            context.write(artist_id, one);
        }
    }

    /*
        Input: <artist_id, 1>
        Outputs: <total songs for that artist, artist_id>
    */
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

            /*
        Input: data from previous job
        Output: <placeholder, artist_id + total ct of songs>
    */
    public static class AccumulatedMapper extends Mapper<Object, Text, Text, Text>{
        private final static Text placeholder = new Text("A");
        private Text outgoingVal = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Read in a line and split based on delimiter
            String line = value.toString();
            String[] items = line.split("\\s+");

            // Output <artist_id, 1> to be summed at the reducer
            String tmpVal = items[0] + " " + items[1];
            outgoingVal.set(tmpVal);

            context.write(placeholder, outgoingVal);
        }
    }

    /*
        Input: <artist_id, total ct of songs>
        Output: <artist with max songs, # that is max songs>
    */
    public static class MaxSongReducer extends Reducer<Text,Text,Text,IntWritable> {
        private Text max_artist = new Text();
        private IntWritable max_ct = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int max = -1;
            for (Text value : values) {
                String[] items = value.toString().split("\\s+");

                String artist_id = items[0];
                int total_ct = Integer.parseInt(items[1]);

                if (total_ct > max) {
                    max_artist.set(artist_id);
                    max_ct.set(total_ct);
                    max = total_ct;
                }
            }

            context.write(max_artist, max_ct);
        }
    }
}