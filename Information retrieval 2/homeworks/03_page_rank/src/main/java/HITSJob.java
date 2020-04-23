import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

public class HITSJob extends Configured implements Tool {
    public static class NodesBuilderMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String link = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(link, "\t");
            int sourceId = Integer.parseInt(tokenizer.nextToken());
            int distId = Integer.parseInt(tokenizer.nextToken());
            context.write(new IntWritable(sourceId), new Text(">" + Integer.toString(distId)));
            context.write(new IntWritable(distId), new Text("<" + Integer.toString(sourceId)));
        }
    }

    public static class NodesBuilderReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> value, Context context) throws  IOException, InterruptedException {
            StringBuilder distResult = new StringBuilder("< 0.0000001");
            StringBuilder sourceResult = new StringBuilder("> 0.0000001");
            for (Text t: value) {
                String link = t.toString();
                if (link.charAt(0) == '<') {
                    distResult.append(' ');
                    distResult.append(link.substring(1));
                } else {
                    sourceResult.append(' ');
                    sourceResult.append(link.substring(1));
                }
            }
            context.write(key, new Text(distResult.toString()));
            context.write(key, new Text(sourceResult.toString()));
        }
    }

    private Job getJobConfBuildNodes(String input, String outDir) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HITSJob.class);
        job.setJobName(HITSJob.class.getCanonicalName() + "[BuildNodes]");

        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(NodesBuilderMapper.class);
        job.setReducerClass(NodesBuilderReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class HITSMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String node = value.toString();
            StringTokenizer tabTokenizer = new StringTokenizer(node, "\t");
            IntWritable vertId = new IntWritable(Integer.parseInt(tabTokenizer.nextToken()));
            StringTokenizer spaceTokenizer = new StringTokenizer(tabTokenizer.nextToken(), " ");
            String direction = spaceTokenizer.nextToken();
            direction = direction + " " + spaceTokenizer.nextToken();
            StringBuilder vertStruct = new StringBuilder(direction);
            while (spaceTokenizer.hasMoreTokens()) {
                int id = Integer.parseInt(spaceTokenizer.nextToken());
                vertStruct.append(" ");
                vertStruct.append(id);
                context.write(new IntWritable(id), new Text(direction));
            }
            vertStruct.append(" ");
            vertStruct.append("#");
            context.write(vertId, new Text(vertStruct.toString()));
        }
    }

    public static class HITSReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double a = 0;
            double h = 0;
            String fromLinks = "";
            String toLinks = "";
            for (Text v: values) {
                StringTokenizer spaceTokenizer = new StringTokenizer(v.toString(), " ");
                String direction = spaceTokenizer.nextToken();
                double t = Double.parseDouble(spaceTokenizer.nextToken());
                if (!spaceTokenizer.hasMoreTokens()) {
                    if (direction.equals("<")) {
                        h += t;
                    } else {
                        a += t;
                    }
                } else {
                    if (direction.equals("<")) {
                        toLinks = spaceTokenizer.nextToken("");
                        toLinks = toLinks.substring(0, toLinks.length() - 2);
                    } else {
                        fromLinks = spaceTokenizer.nextToken("");
                        fromLinks = fromLinks.substring(0, fromLinks.length() - 2);
                    }
                }
            }
            context.write(key, new Text("< " + Double.toString(a) + toLinks));
            context.write(key, new Text("> " + Double.toString(h) + fromLinks));
        }
    }

    private Job getJobConfCountHITS(String input, String outDir, int iter) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HITSJob.class);
        job.setJobName(HITSJob.class.getCanonicalName() + "[CountHITS#" + Integer.toString(iter) + "]");

        FileInputFormat.addInputPath(job, new Path(input + "/part*"));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(HITSMapper.class);
        job.setReducerClass(HITSReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class HITSSortByAuthMapper extends Mapper<Text, Text, DoubleWritable, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer spaceTokenizer = new StringTokenizer(value.toString(), " ");
            if (spaceTokenizer.nextToken().equals("<")) {
                context.write(new DoubleWritable(Double.parseDouble(spaceTokenizer.nextToken())), key);
            }
        }
    }

    public static class HITSSortByHubMapper extends Mapper<Text, Text, DoubleWritable, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int v;
            StringTokenizer spaceTokenizer = new StringTokenizer(value.toString(), " ");
            if (spaceTokenizer.nextToken().equals(">")) {
                context.write(new DoubleWritable(Double.parseDouble(spaceTokenizer.nextToken())), key);
            }
        }
    }

    public static  class DoubleComparator extends WritableComparator {
        public DoubleComparator() {
            super(DoubleWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            Double v1 = ByteBuffer.wrap(b1, s1, l1).getDouble();
            Double v2 = ByteBuffer.wrap(b2, s2, l2).getDouble();
            return v1.compareTo(v2) * -1;
        }
    }

    public static class HITSSortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v: values) {
                context.write(v, new Text(key.toString()));
            }
        }
    }

    private Job getJobConfSortByAuthHITS(String input, String output) throws  IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HITSJob.class);
        job.setJobName(HITSJob.class.getCanonicalName() + "[SortByAuth]");

        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setSortComparatorClass(DoubleComparator.class);
        job.setMapperClass(HITSSortByAuthMapper.class);
        job.setReducerClass(HITSSortReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    private Job getJobConfSortByHubHITS(String input, String output) throws  IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HITSJob.class);
        job.setJobName(HITSJob.class.getCanonicalName() + "[SortByHub]");

        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setSortComparatorClass(DoubleComparator.class);
        job.setMapperClass(HITSSortByHubMapper.class);
        job.setReducerClass(HITSSortReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job initJob = getJobConfBuildNodes(args[0], args[1] + "_iter_0");
        int exitCode = initJob.waitForCompletion(true) ? 0 : 1;
        if (exitCode != 0) {
            return  exitCode;
        }
        int iterNum = Integer.parseInt(args[2]);
        int i = 0;
        for (i = 0; i < iterNum; i++) {
            Job curJob = getJobConfCountHITS(args[1] + "_iter_" + Integer.toString(i), args[1] + "_iter_" + Integer.toString(i + 1), i + 1);
            exitCode = curJob.waitForCompletion(true) ? 0 : 1;
            FileSystem fs = FileSystem.get(curJob.getConfiguration());
            Path lastIterDir = new Path(args[1] + "_iter_" + Integer.toString(i));
            fs.delete(lastIterDir, true);
            if (exitCode != 0) {
                return exitCode;
            }
        }
        Job sortByAuthJob = getJobConfSortByAuthHITS(args[1] + "_iter_" + Integer.toString(i), args[1] + "_sorted_by_auth");
        exitCode = sortByAuthJob.waitForCompletion(true) ? 0 : 1;
        if (exitCode != 0) {
            return exitCode;
        }

        Job sortByHubJob = getJobConfSortByHubHITS(args[1] + "_iter_" + Integer.toString(i), args[1] + "_sorted_by_hub");
        exitCode = sortByHubJob.waitForCompletion(true) ? 0 : 1;

        FileSystem fs = FileSystem.get(sortByAuthJob.getConfiguration());
        Path lastIterDir = new Path(args[1] + "_iter_" + Integer.toString(i));
        fs.delete(lastIterDir, true);

        return exitCode;
    }

    static public void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HITSJob(), args);
        System.exit(exitCode);
    }
}
