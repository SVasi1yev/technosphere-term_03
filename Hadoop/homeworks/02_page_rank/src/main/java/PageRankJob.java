import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

public class PageRankJob extends Configured implements Tool {
    static public final String HANG_VERT_FILE_NAME = "temp_hang_vert.dat";

    public static class NodesBuilderMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        IntWritable negOne = new IntWritable(-1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String link = value.toString();
            if (link.charAt(0) == '#') {
                return;
            }
            StringTokenizer tokenizer = new StringTokenizer(link, "\t");
            IntWritable sourceId = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
            IntWritable distId = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
            context.write(sourceId, distId);
            context.write(distId, negOne);
        }
    }

    public static class NodesBuilderReducer extends Reducer<IntWritable, IntWritable, Text, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder result = new StringBuilder();
            result.append(0.0);
            result.append("|");
            for (IntWritable distId: values) {
                if (distId.get() != -1) {
                    result.append(distId.get());
                    result.append(" ");
                }
            }
            context.write(new Text(key.toString()), new Text(result.toString()));
        }
    }

    private Job getJobConfBuildNodes(String input, String outDir) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName() + "[BuildNodes]");

        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(NodesBuilderMapper.class);
        job.setReducerClass(NodesBuilderReducer.class);
//        job.setNumReduceTasks(4);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class PageRankMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        long vertexNum;
        double hangVertexPageRank = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            vertexNum = context.getConfiguration().getLong("VERTEX_NUM", 0);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String node = value.toString();
            StringTokenizer tabTokenizer = new StringTokenizer(node, "\t");
            IntWritable sourceId = new IntWritable(Integer.parseInt(tabTokenizer.nextToken()));
            StringTokenizer lineTokenizer = new StringTokenizer(tabTokenizer.nextToken(), "|");
            double pageRank = Double.parseDouble(lineTokenizer.nextToken());
            if (pageRank == 0) {
                pageRank = 1 / (double) vertexNum;
            }
            if (!lineTokenizer.hasMoreTokens()) {
                hangVertexPageRank += pageRank;
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

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (hangVertexPageRank == 0) {
                return;
            }
            Configuration config = context.getConfiguration();
            FileSystem fs = FileSystem.get(config);
            Path hangVertFileName = new Path(HANG_VERT_FILE_NAME);
            FSDataOutputStream hangVertFile;
            if (!fs.exists(hangVertFileName)) {
                hangVertFile = fs.create(hangVertFileName);
            } else {
                hangVertFile = fs.append(hangVertFileName);
            }
            hangVertFile.writeDouble(hangVertexPageRank);
            hangVertFile.close();
        }
    }

    public static class PageRankReducer extends Reducer<IntWritable, Text, Text, Text> {
        double regFactor;
        double convCoef = 0.01;
        long vertexNum;
        double hangVertexPageRank = 0;
        final int BUF_SIZE = 1024;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            regFactor = context.getConfiguration().getDouble("REG_FACTOR", 1);
            vertexNum = context.getConfiguration().getLong("VERTEX_NUM", 0);
            Configuration config = context.getConfiguration();
            FileSystem fs = FileSystem.get(config);
            Path hangVertFileName = new Path(HANG_VERT_FILE_NAME);
            if (fs.exists(hangVertFileName)) {
                ByteBuffer buff = ByteBuffer.allocate(BUF_SIZE);
                long fileLen = fs.getFileStatus(hangVertFileName).getLen();
                FSDataInputStream hangVertFile = fs.open(hangVertFileName);
                long procData = 0;
                while (procData < fileLen) {
                    int bytesToRead = (fileLen - procData) > BUF_SIZE ? BUF_SIZE : (int) (fileLen - procData);
                    hangVertFile.readFully(procData, buff.array(), 0, bytesToRead);
                    procData += bytesToRead;
                    for (int i = 0; i < bytesToRead / 4; i++) {
                        hangVertexPageRank += buff.getDouble();
                    }
                }
                hangVertFile.close();
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
                newPageRank += (hangVertexPageRank - oldPageRank) / (double) (vertexNum - 1);
            } else {
                newPageRank += hangVertexPageRank / (double) (vertexNum - 1);
            }
            newPageRank = (1 - regFactor) / ((double) vertexNum) +  regFactor * (newPageRank);
            if ((Math.abs(newPageRank - oldPageRank) / oldPageRank) < convCoef) {
                context.getCounter("COMMON_COUNTERS", "CONV_VERT").increment(1);
            }
            context.write(new Text(key.toString()), new Text(Double.toString(newPageRank) + "|" + links));
        }
    }

    private Job getJobConfCountPageRank(String input, String outDir, double regFactor, int iter) throws IOException {
        Configuration conf = getConf();
        conf.setDouble("REG_FACTOR", regFactor);
        Job job = Job.getInstance(conf);
        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName() + "[CountPageRank#" + Integer.toString(iter) + "]");

        FileInputFormat.addInputPath(job, new Path(input + "/part*"));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
//        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job init_job = getJobConfBuildNodes(args[0], args[1] + "_iter_0");
        int exitCode = init_job.waitForCompletion(true) ? 0 : 1;
        if (exitCode != 0) {
            return exitCode;
        }
        long vertexNum = init_job.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
        double regFactor = Double.parseDouble(args[2]);
        int iterNum = Integer.parseInt(args[3]);
        for (int i = 0; i < iterNum; i++) {
            Job curJob = getJobConfCountPageRank(args[1] + "_iter_" + Integer.toString(i), args[1] + "_iter_" + Integer.toString(i + 1), regFactor, i + 1);
            curJob.getConfiguration().setLong("VERTEX_NUM", vertexNum);
            exitCode = curJob.waitForCompletion(true) ? 0 : 1;
            FileSystem fs = FileSystem.get(curJob.getConfiguration());
            Path lastIterDir = new Path(args[1] + "_iter_" + Integer.toString(i));
            Path hangVertFileName = new Path(HANG_VERT_FILE_NAME);
            if (fs.exists(hangVertFileName)) {
                fs.delete(hangVertFileName, false);
            }
            fs.delete(lastIterDir, true);
            if (exitCode != 0) {
                return exitCode;
            }
            long convVert = curJob.getCounters().findCounter("COMMON_COUNTERS", "CONV_VERT").getValue();
            System.out.println(convVert);
            if (convVert == vertexNum) {
                break;
            }
        }
        return exitCode;
    }

    static public void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PageRankJob(), args);
        System.exit(exitCode);
    }
}
