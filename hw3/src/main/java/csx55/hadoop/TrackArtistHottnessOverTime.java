import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrackArtistHottnessOverTime {
    /*
        Output artist ID + year and hotness score with the year
    */
    public static class HottnessMapper extends Mapper<Object, Text, Text, DoubleWritable>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\\|"); 

            try{
                String artist_id = items[34];
                int year = Integer.parseInt(items[items.length - 1]);
                double hottness = Double.parseDouble(items[1]);
                if (year >= 2000 && year <= 2010){
                    String tmp = artist_id + " " + year;
                    context.write(new Text(tmp), new DoubleWritable(hottness));
                }
            } catch (NumberFormatException nfe) {
                // Do nothing
            }
        }
    }

    /*
        Output the artist's average hotness per year
    */
    public static class HottnessReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int ct = 0;

            for(DoubleWritable value : values) {
                sum += value.get();
                ct++;
            }

            double avg = (sum / ct);
            context.write(key, new DoubleWritable(avg));
        }
    }

    /*
        Maps <artist_id, year + avg score>
    */
    public static class GrowthMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\\s+");

            String tmp = "" + items[1] + " " + items[2];
            context.write(new Text(items[0]), new Text(tmp));
        }
    }

    /*
        Determines min year and max year available for an artist.
    */
    public static class GrowthReducer extends Reducer<Text,Text,DoubleWritable,Text> {
        private DoubleWritable outVal = new DoubleWritable();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> year_hottness_map = new HashMap<>();
            int minYear = Integer.MAX_VALUE;
            int maxYear = Integer.MIN_VALUE;

            for (Text value : values) {
                String[] items = value.toString().split(" ");

                int year = Integer.parseInt(items[0]);
                double hottness = Double.parseDouble(items[1]);

                String year_key = "" + year;
                year_hottness_map.put(year_key, hottness);

                if (year < minYear) {
                    minYear = year;
                }

                if (year > maxYear) {
                    maxYear = year;
                }
            }       

            String minYear_key = "" + minYear;
            double minYear_val = year_hottness_map.get(minYear_key);

            String maxYear_key = "" + maxYear;
            double maxYear_val = year_hottness_map.get(maxYear_key);

            double percentChange = ((maxYear_val - minYear_val) / maxYear_val) * 100.0;


            if (!Double.isNaN(percentChange) && !Double.isInfinite(percentChange)) {
                outVal.set(percentChange);
                context.write(outVal, key);
            }
        }
    }

    /*
        Input: <percent change, artist ID>
        Output: Map percent change and artist ID
    */
    public static class MaxMapper extends Mapper<Object, Text, Text, Text>{
        private Text placeHolder = new Text("A");
        private Text outVal = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\\s+");

            String tmp = "" + items[0] + " " + items[1];
            outVal.set(tmp);

            context.write(placeHolder, outVal);
        }
    }

    public static class MaxReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double max_percent = Double.MIN_VALUE;
            String max_artist = "ERROR";

            for (Text value : values) {
                String[] items = value.toString().split(" ");

                double percent = Double.parseDouble(items[0]);
                String artist_id = items[1];

                if (percent > max_percent) {
                    max_percent = percent;
                    max_artist = artist_id;
                }
            }

            String tmp = "" + max_percent;
            context.write(new Text(max_artist), new Text(tmp));
        }
    }
}
