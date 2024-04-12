import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;

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

public class MostGenericArtist {
    public static class GenericArtistMapper extends Mapper<Object, Text, DoubleWritable, Text>{
        private Text outID = new Text();
        private DoubleWritable outScore = new DoubleWritable();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\\|"); 

            double artistScore = calculateArtistScore(items[11], items[12]);
            String artist_id = items[2];

            outScore.set(artistScore);
            outID.set(artist_id);
            context.write(outScore, outID);
        }
    }

    private static double calculateArtistScore(String term_frequency, String term_weight) {
        // Remove square brackets and split by whitespace
        String[] term_freqs = term_frequency.replaceAll("[\\[\\]]", "").split("\\s+");
        String[] term_weights = term_weight.replaceAll("[\\[\\]]", "").split("\\s+");

        double score = 0.0;
        for (int i = 0; i < term_freqs.length; i++) {
            try{
                double freq = Double.parseDouble(term_freqs[i]);
                double weight = Double.parseDouble(term_weights[i]);

                score += (freq * weight);
            } catch (NumberFormatException nfe) {
                // do nothing
            }
        }

        return score;
    }

    public static class GenericArtistReducer extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Finding Most Generic Artist");

        job.setJarByClass(MostGenericArtist.class);
        job.setMapperClass(GenericArtistMapper.class);
        job.setReducerClass(GenericArtistReducer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(SongDurations.Comparator.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
