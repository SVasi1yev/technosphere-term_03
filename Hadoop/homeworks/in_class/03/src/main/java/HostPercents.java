/*
 * Задача: по заданному access.log вывести проценты обращений к доменам.
 * Специально разделим задачу на 2 части:
 *  - часть #1: wordcount по доменам
 *  - часть #2: map считает частичные суммы,
 *              reducer - выводит проценты по каждому домену
 */
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class HostPercents extends Configured implements Tool {
    public static class HostPercentsMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                URL url = new URL(value.toString());
                context.write(new Text(url.getHost()), one);
            } catch (MalformedURLException e) {
                context.getCounter("COMMON_COUNTERS", "MalformedUrls").increment(1);
            }
        }
    }

    public static class HostPercentsReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable x: values) {
                sum += x.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static class HostPercentsMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] host_cnt = value.toString().split("\t");
            int cnt = Integer.valueOf(host_cnt[1]);
            context.write(new Text("H:"+host_cnt[0]), new IntWritable(cnt));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            /* Этот код вызывается при финализации mapper-а */
        }
    }

    public static class HostPercentsReducer2 extends Reducer<Text, IntWritable, Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /* НАПИШИТЕ ЗДЕСЬ СВОЙ КОД */
            /* Ваш вывод должен быть в формате HOST (Text) -> percent (FloatWritable) */
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String input = args[0];
        String out_dir = args[1];

        // Первая часть представляет из себя буквально WordCount
        ControlledJob step1 = new ControlledJob(getConf());
        step1.setJob(GetStep1Job(input, out_dir + "/step1"));

        // Во второй части мы подсчитываем долю каждого домена
        ControlledJob step2 = new ControlledJob(getConf());
        step2.setJob(GetStep2Job(out_dir + "/step1/part-r-*", out_dir + "/step2"));

        JobControl control = new JobControl(HostPercents.class.getCanonicalName());
        control.addJob(step1);
        control.addJob(step2);
        step2.addDependingJob(step1);

        new Thread(control).start();
        while (!control.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(2000);
        }

        return control.getFailedJobList().isEmpty() ? 0 : 1;
    }

    Job GetStep1Job(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HostPercents.class);
        job.setJobName(HostPercents.class.getCanonicalName() + " [step 1]");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(HostPercentsMapper1.class);
        job.setCombinerClass(HostPercentsReducer1.class);
        job.setReducerClass(HostPercentsReducer1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    Job GetStep2Job(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HostPercents.class);
        job.setJobName(HostPercents.class.getCanonicalName() + "[step 2]");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(HostPercentsMapper2.class);
        job.setReducerClass(HostPercentsReducer2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new HostPercents(), args);
        System.exit(rc);
    }
}
