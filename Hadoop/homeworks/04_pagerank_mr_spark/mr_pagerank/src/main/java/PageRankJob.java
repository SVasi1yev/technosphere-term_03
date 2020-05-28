import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

public class PageRankJob extends Configured implements Tool {
    static public final String HANG_VERT_FILE_NAME = "tempHangVert";
    static public final int VERT_NUM = 4847571;

    public static class NodesBuilderMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        IntWritable nullId = new IntWritable(-1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String link = value.toString();
            if (link.charAt(0) == '#') {
                return;
            }
            StringTokenizer tokenizer = new StringTokenizer(link, "\t");
            int sourceId = Integer.parseInt(tokenizer.nextToken());
            int distId = Integer.parseInt(tokenizer.nextToken());
            context.write(new IntWritable(sourceId), new IntWritable(distId));
            context.write(new IntWritable(distId), nullId);
        }
    }

    public static class NodesBuilderReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        long hangVertNum = 0;

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder result = new StringBuilder();
            result.append(0.0);
            result.append("|");
            boolean hangVert = true;
            for (IntWritable distId: values){
                if (distId.get() != -1) {
                    hangVert = false;
                    result.append(distId.toString()); result.append(" ");
                }
            }
            if (hangVert) {
                hangVertNum++;
            }
            context.write(key, new Text(result.toString()));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            MultipleOutputs<IntWritable, Text> out = new MultipleOutputs<>(context);
            out.write(HANG_VERT_FILE_NAME, new IntWritable(0), new Text(Long.toString(hangVertNum)));
            out.close();
        }
    }

    private Job getJobConfBuildNodes(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName() + "[BuildNodes]");

        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleOutputs.addNamedOutput(job, HANG_VERT_FILE_NAME, TextOutputFormat.class, IntWritable.class, Text.class);

        job.setMapperClass(NodesBuilderMapper.class);
        job.setReducerClass(NodesBuilderReducer.class);
        job.setNumReduceTasks(8);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class PageRankMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String node = value.toString();
            StringTokenizer tabTokenizer = new StringTokenizer(node, "\t");
            IntWritable sourceId = new IntWritable(Integer.parseInt(tabTokenizer.nextToken()));
            StringTokenizer lineTokenizer = new StringTokenizer(tabTokenizer.nextToken(), "|");
            double pageRank = Double.parseDouble(lineTokenizer.nextToken());
            if (pageRank == 0) {
                pageRank = 1 / (double) VERT_NUM;
            }
            if (!lineTokenizer.hasMoreTokens()) {
                context.write(sourceId, new Text(pageRank + "|#"));
            } else {
                String links = lineTokenizer.nextToken();
                context.write(sourceId, new Text(pageRank + "|" + links));
                StringTokenizer spaceTokenizer = new StringTokenizer(links, " ");
                int linksNum = spaceTokenizer.countTokens();
                double pageRankForLink = pageRank / linksNum;
                while (spaceTokenizer.hasMoreTokens()) {
                    context.write(new IntWritable(Integer.parseInt(spaceTokenizer.nextToken())), new Text(Double.toString(pageRankForLink)));
                }
            }
        }
    }

    public static class PageRankReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        double regFactor;
        double hangVertexPageRank = 0;
        double newHangVertPageRank = 0;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration config = context.getConfiguration();
            regFactor = config.getDouble("REG_FACTOR", 1);

            FileSystem fs = FileSystem.get(config);
            Path lastIterDirPath = new Path(config.get("LAST_DIR_PATH"));
            for (FileStatus file: fs.listStatus(lastIterDirPath)) {
                if (file.getPath().toString().contains(HANG_VERT_FILE_NAME)) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                    while (br.ready()) {
                        String curLine = br.readLine();
                        double pr = Double.parseDouble(curLine.split("\t")[1]);
                        if (pr >= 1) {
                            pr /= VERT_NUM;
                        }
                        hangVertexPageRank += pr;
                    }
                }
            }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            double oldPageRank = 0;
            String links = "";
            double newPageRank = 0;
            boolean hangVert = false;
            for (Text node: value) {
                StringTokenizer lineTokenizer = new StringTokenizer(node.toString(), "|");
                double curPageRank = Double.parseDouble(lineTokenizer.nextToken());
                if (!lineTokenizer.hasMoreTokens()) {
                    newPageRank += curPageRank;
                } else {
                    oldPageRank = curPageRank;
                    links = lineTokenizer.nextToken();
                    if (links.charAt(0) == '#') {
                        links = "";
                        hangVert = true;
                    }
                }
            }
            if (hangVert) {
                newPageRank += hangVertexPageRank / (double) VERT_NUM;
            } else {
                newPageRank += hangVertexPageRank / (double) VERT_NUM;
            }
            newPageRank = (1 - regFactor) / ((double) VERT_NUM) +  regFactor * (newPageRank);
            if (hangVert) {
                newHangVertPageRank += newPageRank;
            }
            context.write(key, new Text(newPageRank + "|" + links));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            MultipleOutputs<IntWritable, Text> out = new MultipleOutputs<>(context);
            out.write(HANG_VERT_FILE_NAME, new IntWritable(0), new Text(Double.toString(newHangVertPageRank)));
            out.close();
        }
    }

    private Job getJobConfCountPageRank(String input, String output, double regFactor, int iter) throws IOException {
        Configuration conf = getConf();
        conf.setDouble("REG_FACTOR", regFactor);
        Job job = Job.getInstance(conf);
        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName() + "[CountPageRank#" + iter + "]");

        FileInputFormat.addInputPath(job, new Path(input + "/part*"));
        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleOutputs.addNamedOutput(job, HANG_VERT_FILE_NAME, TextOutputFormat.class, IntWritable.class, Text.class);

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
	job.setNumReduceTasks(8);	

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class PageRankSortMapper extends Mapper<Text, Text, DoubleWritable, IntWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new DoubleWritable(Double.parseDouble(value.toString().split("\\|")[0])), new IntWritable(Integer.parseInt(key.toString())));
        }
    }

    public static class DoubleComparator extends WritableComparator {
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

    public static class PageRankSortReducer extends Reducer<DoubleWritable, IntWritable, IntWritable, DoubleWritable> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable v: values) {
                context.write(v, key);
            }
        }
    }

    private Job getJobConfSortPageRank(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName() + "[Sort]");

        KeyValueTextInputFormat.addInputPath(job, new Path(input + "/part*"));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setSortComparatorClass(DoubleComparator.class);
        job.setMapperClass(PageRankSortMapper.class);
        job.setReducerClass(PageRankSortReducer.class);
	job.setNumReduceTasks(8);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job initJob = getJobConfBuildNodes(args[0], args[1] + "_iter_0");
        int exitCode = initJob.waitForCompletion(true) ? 0 : 1;
        if (exitCode != 0) {
            return exitCode;
        }
        double regFactor = Double.parseDouble(args[2]);
        int iterNum = Integer.parseInt(args[3]);
        int i;
        for (i = 0; i < iterNum; i++) {
            Job curJob = getJobConfCountPageRank(args[1] + "_iter_" + i, args[1] + "_iter_" + (i + 1), regFactor, i + 1);
            curJob.getConfiguration().set("LAST_DIR_PATH", args[1] + "_iter_" + i);
            exitCode = curJob.waitForCompletion(true) ? 0 : 1;
            FileSystem fs = FileSystem.get(curJob.getConfiguration());
            Path lastIterDir = new Path(args[1] + "_iter_" +i);
            Path hangVertFileName = new Path(HANG_VERT_FILE_NAME);
            if (fs.exists(hangVertFileName)) {
                fs.delete(hangVertFileName, false);
            }
            fs.delete(lastIterDir, true);
            if (exitCode != 0) {
                return exitCode;
            }
        }
        Job sortJob = getJobConfSortPageRank(args[1] + "_iter_" + i, args[1] + "_sorted");
        exitCode = sortJob.waitForCompletion(true) ? 0 : 1;
        FileSystem fs = FileSystem.get(sortJob.getConfiguration());
        Path lastIterDir = new Path(args[1] + "_iter_" + i);
        fs.delete(lastIterDir, true);
        return exitCode;
    }

    static public void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PageRankJob(), args);
        System.exit(exitCode);
    }
}
