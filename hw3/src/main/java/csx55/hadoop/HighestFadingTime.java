

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

public class HighestFadingTime {
    public static class FadingTimeMapper extends Mapper<Object, Text, Text, Text>{
        private Text placeholder = new Text("A");
        private Text value = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\\|");
            
            double time_fading_in = Double.parseDouble(items[5]);
            double duration = Double.parseDouble(items[4]);
            double time_fading_out = Double.parseDouble(items[12]) - duration;

            double total_time_fading = time_fading_in + time_fading_out;

            // <artist_id, total time fading>
            String outVal = items[34] + " " + total_time_fading;
            value.set(outVal);

            context.write(placeholder, value);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Artist with Highest Fading Time In Dataset");

        job.setJarByClass(HighestFadingTime.class);
        job.setMapperClass(FadingTimeMapper.class);
        job.setReducerClass(LoudestAverageSongs.MaxDoubleReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}