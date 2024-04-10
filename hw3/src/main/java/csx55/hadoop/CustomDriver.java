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

public class CustomDriver extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CustomDriver(), args);
    	System.exit(res); //res will be 0 if all tasks are executed succesfully and 1 otherwise
    }

    @Override
   	public int run(String[] args) throws Exception {
        // JOB 1
        Configuration conf = this.getConf();
        // Job job = Job.getInstance(conf, "Count Songs in Period 2000 through 2010");

        // job.setJarByClass(CustomDriver.class);
        // job.setMapperClass(CountSongsInTimePeriod.CountMapper.class);
        // job.setReducerClass(CountSongsInTimePeriod.CountReducer.class);

        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(IntWritable.class);

        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(IntWritable.class);

        // job.setSortComparatorClass(CountSongsInTimePeriod.Comparator.class);

        // FileInputFormat.addInputPath(job, new Path(args[1]));
        // FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // job.waitForCompletion(true);

        // Job job2 = Job.getInstance(conf, "Calculate avg hottness per year for an artist");

        // job2.setJarByClass(CustomDriver.class);
        // job2.setMapperClass(TrackArtistHottnessOverTime.HottnessMapper.class);
        // job2.setReducerClass(TrackArtistHottnessOverTime.HottnessReducer.class);

        // job2.setMapOutputKeyClass(Text.class);
        // job2.setMapOutputValueClass(DoubleWritable.class);

        // job2.setOutputKeyClass(Text.class);
        // job2.setOutputValueClass(DoubleWritable.class);

        // FileInputFormat.addInputPath(job2, new Path("/hw3/combined.txt"));
        // FileOutputFormat.setOutputPath(job2, new Path("/hw3/avgHottness_final"));
        // job2.waitForCompletion(true);

        // Job job3 = Job.getInstance(conf, "Calculate Percent Change for Artists over a year period");

        // job3.setJarByClass(CustomDriver.class);
        // job3.setMapperClass(TrackArtistHottnessOverTime.GrowthMapper.class);
        // job3.setReducerClass(TrackArtistHottnessOverTime.GrowthReducer.class);

        // job3.setMapOutputKeyClass(Text.class);
        // job3.setMapOutputValueClass(Text.class);

        // job3.setOutputKeyClass(DoubleWritable.class);
        // job3.setOutputValueClass(Text.class);

        // job3.setSortComparatorClass(SongDurations.Comparator.class);

        // FileInputFormat.addInputPath(job3, new Path("/hw3/avgHottness_final"));
        // FileOutputFormat.setOutputPath(job3, new Path("/hw3/percentChange_final"));
        // job3.waitForCompletion(true);
        
        Job job4 = Job.getInstance(conf, "Finding Max of Percents");

        job4.setJarByClass(CustomDriver.class);
        job4.setMapperClass(TrackArtistHottnessOverTime.MaxMapper.class);
        job4.setReducerClass(TrackArtistHottnessOverTime.MaxReducer.class);

        job4.setMapOutputKeyClass(DoubleWritable.class);
        job4.setMapOutputValueClass(Text.class);

        job4.setOutputKeyClass(DoubleWritable.class);
        job4.setOutputValueClass(Text.class);

        job4.setSortComparatorClass(SongDurations.Comparator.class);

        FileInputFormat.addInputPath(job4, new Path("/hw3/percentChange_final"));
        FileOutputFormat.setOutputPath(job4, new Path("/hw3/maxPercent_final"));
        job4.waitForCompletion(true);

        return 1;
   	}
}
