import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SongDurations {
    public static class SortMapper extends Mapper<Object, Text, DoubleWritable, Text>{
        private Text song_id = new Text();
        private DoubleWritable duration = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\\|");
            
            duration.set(Double.parseDouble(items[4]));
            song_id.set(items[0]);

            context.write(duration, song_id);
        }
    }

    public static class SortReducer extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sorting File by Durations");

        job.setJarByClass(SongDurations.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(Comparator.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class Comparator extends WritableComparator {
        protected Comparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DoubleWritable aa = (DoubleWritable) a;
            DoubleWritable bb = (DoubleWritable) b;
            return -1 * aa.compareTo(bb); // Sort in descending order
        }
    }
}
