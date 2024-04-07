import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TextFileJoin {
    public static class AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input record based on the delimiter
            String[] fields = value.toString().split("\\|");
            // Extract song_id as the key
            String song_id = fields[0];
            // Emit key-value pair with song_id as key and entire record as value
            context.write(new Text(song_id), new Text("analysis|" + value.toString()));
        }
    }

    public static class MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input record based on the delimiter
            String[] fields = value.toString().split("\\|");
            // Extract song_id as the key
            String song_id = fields[7];

            // Construct the output value without song_id
            StringBuilder outVal = new StringBuilder();
            for (int i = 0; i < fields.length; i++) {
                if (i != 7) {
                    outVal.append(fields[i]);
                    if (i < fields.length - 1) {
                        outVal.append("|");
                    }
                }
            }

            // Emit key-value pair with song_id as key and entire record as value
            context.write(new Text(song_id), new Text("metadata|" + outVal.toString()));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String analysis = null;
            String metadata = null;
            // Iterate through all values for a given song_id
            for (Text value : values) {
                String[] fields = value.toString().split("\\|");
                // Determine whether the record comes from analysis.txt or metadata.txt based on its prefix
                if (fields[0].equals("analysis")) {
                    analysis = value.toString().substring("analysis|".length());
                } else if (fields[0].equals("metadata")) {
                    metadata = value.toString().substring("metadata|".length());
                }
            }
            // If both analysis and metadata are found, emit joined record
            if (analysis != null && metadata != null) {
                context.write(new Text(""), new Text(analysis + "|" + metadata));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Text File Join");
        job.setJarByClass(TextFileJoin.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add multiple inputs with different mappers
        MultipleInputs.addInputPath(job, new Path("/hw3/analysis.txt"), TextInputFormat.class, AnalysisMapper.class);
        MultipleInputs.addInputPath(job, new Path("/hw3/metadata.txt"), TextInputFormat.class, MetadataMapper.class);

        FileOutputFormat.setOutputPath(job, new Path("/hw3/combined_final"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
