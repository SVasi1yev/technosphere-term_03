import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class HITSJob extends Configured implements Tool {
    public static class NodesBuilderMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String link = value.toString();
            if (link.charAt(0) == '#') {
                return;
            }
            StringTokenizer tokenizer = new StringTokenizer(link, "\t");
            int sourceId = Integer.parseInt(tokenizer.nextToken());
            int distId = Integer.parseInt(tokenizer.nextToken());
            context.write(new IntWritable(sourceId), new Text(">" + Integer.toString(distId)));
            context.write(new IntWritable(distId), new Text("<" + Integer.toString(sourceId)));
        }
    }

    public static class NodesBuilderReducer extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> value, Context context) throws  IOException, InterruptedException {
            StringBuilder distResult = new StringBuilder("< 1");
            StringBuilder sourceResult = new StringBuilder("> 1");
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
            context.write(new Text(key.toString()), new Text(distResult.toString()));
            context.write(new Text(key.toString()), new Text(sourceResult.toString()));
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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class HITSMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) {
            String node = value.toString();
            StringTokenizer tabTokenizer = new StringTokenizer(node, "\t");
            IntWritable vertId = new IntWritable(Integer.parseInt(tabTokenizer.nextToken()));
            StringTokenizer spaceTokenizer = new StringTokenizer(tabTokenizer.nextToken());
            String direction = spaceTokenizer.nextToken();
            String
        }
    }

    private Job getJobConfCountPageRank(String input, String outDir) throws IOException {
        Job job = Job.getInstance(getConf());

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConfBuildNodes(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HITSJob(), args);
        System.exit(exitCode);
    }
}
