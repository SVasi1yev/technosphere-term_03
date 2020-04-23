import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;

public class SEOJob extends Configured implements Tool {
    public static class SEOMapperFirst extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] queryHost = value.toString().split("\t");

            if (queryHost.length != 2) {
                context.getCounter("COMMON_COUNTERS", "WeakRecords").increment(1);
            } else {
                try {
                    URL url = new URL(queryHost[1]);
                    context.write(new Text(url.getHost() + "\t" + queryHost[0]), ONE);
                } catch (MalformedURLException e) {
                    context.getCounter("COMMON_COUNTERS", "WeakUrls").increment(1);
                }
            }
        }
    }

    public static class SEOReducerFirst extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long result = 0;
            for (LongWritable v: values) {
                result += v.get();
            }
            context.write(key, new LongWritable(result));
        }
    }

    private Job getSEOJobFirst(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(SEOJob.class);
        job.setJobName(SEOJob.class.getCanonicalName() + "#1");

        FileInputFormat.addInputPath(job, new Path(input));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(output + "_tmp"));

        job.setMapperClass(SEOMapperFirst.class);
        job.setCombinerClass(SEOReducerFirst.class);
        job.setReducerClass(SEOReducerFirst.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job;
    }

    public static class SEOPartitioner extends Partitioner<HostCount, Text> {
        @Override
        public int getPartition(HostCount hostCount, Text text, int numPartitions) {
            return Math.abs(hostCount.hashCode()) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(HostCount.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((HostCount) a).compareTo((HostCount) b);
        }
    }

    public static class SEOGrouper extends WritableComparator {
        protected SEOGrouper() {
            super(HostCount.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text host1 = ((HostCount) a).getHost();
            Text host2 = ((HostCount) b).getHost();
            return host1.compareTo(host2);
        }
    }

    public static class SEOMapperSecond extends Mapper<LongWritable, Text, HostCount, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splited = value.toString().split("\t");
            context.write(new HostCount(new Text(splited[0]), new LongWritable(Integer.parseInt(splited[2]))), new Text(splited[1]));
        }
    }

    public static class SEOReducerSecond extends Reducer<HostCount, Text, Text, LongWritable> {
        private int minClicks = 1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minClicks = context.getConfiguration().getInt("seo.minclicks", minClicks);
        }

        @Override
        protected void reduce(HostCount key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String query = values.iterator().next().toString();
            LongWritable count = key.getCount();
            if (count.get() >= minClicks) {
                context.write(new Text(key.getHost() + "\t" + query), count);
            }
        }
    }

    private Job getSEOJobSecond(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(SEOJob.class);
        job.setJobName(SEOJob.class.getCanonicalName() + "#2");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(SEOMapperSecond.class);
        job.setReducerClass(SEOReducerSecond.class);

        job.setPartitionerClass((SEOPartitioner.class));
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(SEOGrouper.class);

        job.setMapOutputKeyClass(HostCount.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(HostCount.class);
        job.setOutputValueClass(LongWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job jobFirst = getSEOJobFirst(args[0], args[1]);
        int exitCode = jobFirst.waitForCompletion(true ) ? 0 : 1;
        if (exitCode != 0) {
            return exitCode;
        }
        Job jobSecond = getSEOJobSecond(args[1] + "_tmp", args[1]);
        exitCode = jobSecond.waitForCompletion(true) ? 0 : 1;
        FileSystem fs = FileSystem.get(jobSecond.getConfiguration());
        if (fs.exists(new Path(args[1] + "_tmp"))) {
            fs.delete(new Path(args[1] + "_tmp"));
        }
        return exitCode;
    }

    static public void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SEOJob(), args);
        System.exit(exitCode);
    }
}
