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

public class SegmentDataAvg {
    public static class SegmentAvgMapper extends Mapper<Object, Text, Text, Text>{
        private Text placeholder = new Text("A");
        private Text outVal = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\\|");

            // Parse the space-separated arrays from the original text and convert to doubles
            double segStartAvg = calculateAvg(items[17]);
            double segConfidenceAvg = calculateAvg(items[18]);
            double segPitchesAvg = calculateAvg(items[19]);
            double segTimbreAvg = calculateAvg(items[20]);
            double segLoudnessMaxAvg = calculateAvg(items[21]);
            double segLoudnessMaxTimeAvg = calculateAvg(items[22]);
            double segLoudnessStartAvg = calculateAvg(items[23]);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(segStartAvg).append(" ")
                        .append(segConfidenceAvg).append(" ")
                        .append(segPitchesAvg).append(" ")
                        .append(segTimbreAvg).append(" ")
                        .append(segLoudnessMaxAvg).append(" ")
                        .append(segLoudnessMaxTimeAvg).append(" ")
                        .append(segLoudnessStartAvg);

            outVal.set(stringBuilder.toString());
            context.write(placeholder, outVal);
        }
    }

    private static double calculateAvg(String segmentData) {
        // Remove square brackets and split by whitespace
        String[] values = segmentData.replaceAll("[\\[\\]]", "").split("\\s+");
        double sum = 0.0;
        int count = 0;
        for (String value : values) {
            try {
                sum += Double.parseDouble(value);
                count++;
            } catch (NumberFormatException nfe) {
                // Do nothing
            }
        }
        return count > 0 ? sum / count : 0.0; // Return average or 0 if no values
    }

    public static class SegmentAvgReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double segStartSum = 0.0, segStartCt = 0.0;
            double segConfidenceSum = 0.0, segConfidenceCt = 0.0;
            double segPitchesSum = 0.0, segPitchesCt = 0.0;
            double segTimbreSum = 0.0, segTimbreCt = 0.0;
            double segLoudnessMaxSum = 0.0, segLoudnessMaxCt = 0.0;
            double segLoudnessMaxTimeSum = 0.0, segLoudnessMaxTimeCt = 0.0;
            double segLoudnessStartSum = 0.0, segLoudnessStartCt = 0.0;

            for (Text value : values) {
                // Add sum to each sum
                double[] items = Arrays.stream(value.toString().split("\\s+"))
                                .mapToDouble(Double::parseDouble)
                                .toArray();

                // Add to all the sums
                segStartSum += items[0]; segConfidenceSum += items[1]; segPitchesSum += items[2];
                segTimbreSum += items[3]; segLoudnessMaxSum += items[4]; segLoudnessMaxTimeSum += items[5];
                segLoudnessStartSum += items[6];

                // Increment all counts by 1
                segStartCt++; segConfidenceCt++; segPitchesCt++; segTimbreCt++; segLoudnessMaxCt++; segLoudnessMaxTimeCt++; segLoudnessStartCt++;
            }

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("segStartAvg:\t" + (segStartSum/segStartCt)).append("|")  
                        .append("segConfidenceAvg:\t" + (segConfidenceSum/segConfidenceCt)).append("|")
                        .append("segPitchesAvg:\t" + (segPitchesSum/segPitchesCt)).append("|")
                        .append("segTimbreAvg:\t" + (segTimbreSum/segTimbreCt)).append("|")
                        .append("segLoudnessMaxAvg:\t" + (segLoudnessMaxSum/segLoudnessMaxCt)).append("|")
                        .append("segLoudnessMaxTimeAvg:\t" + (segLoudnessMaxTimeSum/segLoudnessMaxTimeCt)).append("|")
                        .append("segLoudnessStartSum:\t" + (segLoudnessStartSum/segLoudnessStartCt));
            String outval = stringBuilder.toString();

            context.write(new Text(outval), new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sorting File by Durations");

        job.setJarByClass(SegmentDataAvg.class);
        job.setMapperClass(SegmentAvgMapper.class);
        job.setReducerClass(SegmentAvgReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
