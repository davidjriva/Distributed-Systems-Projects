import java.io.IOException;
import java.util.StringTokenizer;
import java.util.PriorityQueue;

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

public class MostEnergyAndDance {
    public static class TopNMapper extends Mapper<Object, Text, Text, Text> {
        private Text placeholder = new Text("A");
        private Text outVal = new Text();
    
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\\|");

            String song_id = items[0];

            double energy = Double.parseDouble(items[6]);
            double danceability = Double.parseDouble(items[3]);
            double score = energy + danceability;

            String val = song_id + " " + score;
            outVal.set(val);

            context.write(placeholder, outVal);
        }
    }


    public static class Song {
        private String name;
        private double score;

        public Song(String name, double score) {
            this.name = name;
            this.score = score;
        }

        public String getName() {
            return name;
        }

        public double getScore() {
            return score;
        }
    }

    public static class TopNReducer extends Reducer<Text,Text,Text,DoubleWritable> {
        private PriorityQueue<Song> maxHeap = new PriorityQueue<>((a, b) -> Double.compare(b.getScore(), a.getScore()));
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] items = value.toString().split(" ");

                Song s = new Song(items[0], Double.parseDouble(items[1]));

                maxHeap.offer(s);

                if (maxHeap.size() > 10) {
                    maxHeap.poll();
                }
            }

            while (!maxHeap.isEmpty()) {
                Song s = maxHeap.poll();

                String song_id = s.getName();
                double score = s.getScore();

                context.write(new Text(song_id), new DoubleWritable(score));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Songs with Best Dance And Energy");

        job.setJarByClass(MostEnergyAndDance.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}