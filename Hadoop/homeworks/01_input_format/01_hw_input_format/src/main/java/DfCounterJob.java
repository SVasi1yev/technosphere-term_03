import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DfCounterJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
//        System.err.println("Start");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class DfCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        static final IntWritable one = new IntWritable(1);
        HashSet<String> processedWords = new HashSet<String>();
        String word;

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
//            System.err.println("MAPPER: " + key.toString());
            String document = value.toString().toLowerCase();
            Pattern pattern = Pattern.compile("\\p{L}+");
            Matcher matcher = pattern.matcher(document);

            while (matcher.find()) {
                word = matcher.group();
                if (!processedWords.contains(word)) {
                    processedWords.add(word);
                    context.write(new Text(word), one);
                }
            }
            processedWords.clear();
        }
    }

    public static class DfCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text word, Iterable<IntWritable> nums, Context context) throws IOException, InterruptedException {
            int result = 0;
            for (IntWritable value: nums) {
                result += value.get();
            }

            context.write(word, new IntWritable(result));
        }
    }

    private Job getJobConf(String input, String outDir) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(DfCounterJob.class);
        job.setJobName(DfCounterJob.class.getCanonicalName());

        job.setInputFormatClass(PKZInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(DfCounterMapper.class);
        job.setReducerClass(DfCounterReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DfCounterJob(), args);
        System.exit(exitCode);
    }
}
