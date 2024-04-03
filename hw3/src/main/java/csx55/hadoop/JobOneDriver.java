import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class JobOneDriver extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JobOneDriver(), args);
    	System.exit(res); //res will be 0 if all tasks are executed succesfully and 1 otherwise
    }

    @Override
   	public int run(String[] args) throws Exception {
        // JOB 1
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Accumulate Songs for Artist");

        job.setJarByClass(JobOneDriver.class);
        job.setMapperClass(MaxSongsInDataset.TokenizerMapper.class);
        job.setReducerClass(MaxSongsInDataset.IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);

        // ------------------- JOB 2 ----------------------

        Job job2 = Job.getInstance(conf, "Find Artist with Max Songs");

        job2.setJarByClass(JobOneDriver.class);
        job2.setMapperClass(MaxSongsInDataset.AccumulatedMapper.class);
        job2.setReducerClass(MaxSongsInDataset.MaxSongReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path("/hw3/maxsongs/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path("/hw3/maxsongs_final"));

        job2.waitForCompletion(true);
        return 1;
   	}
}
