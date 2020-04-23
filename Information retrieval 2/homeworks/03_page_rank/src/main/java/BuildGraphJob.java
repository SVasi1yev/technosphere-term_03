import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class BuildGraphJob extends Configured implements Tool {
    public static LinkedList<String> getLinks(String data) throws IOException {
        byte[] decoded = Base64.getDecoder().decode(data);

        Inflater inflater = new Inflater();
        inflater.setInput(decoded);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] outBuffer= new byte[1024];
        while (!inflater.finished()) {
            int bytesInflated;
            try {
                bytesInflated = inflater.inflate(outBuffer);
            } catch (DataFormatException e) {
                throw new RuntimeException(e.getMessage());
            }

            baos.write(outBuffer, 0, bytesInflated);
        }
        String HTMLPage = baos.toString("UTF-8");

        Pattern linkPattern = Pattern.compile("<a[^>]+href=[\"']?([^\"'\\s>]+)[\"']?[^>]*>",
                Pattern.CASE_INSENSITIVE|Pattern.DOTALL);
        Matcher pageMatcher = linkPattern.matcher(HTMLPage);
        LinkedList<String> links = new LinkedList<>();
        while (pageMatcher.find()) {
            links.add(pageMatcher.group(1));
        }
        return links;
    }

    static public class BuildGraphMapperLinks extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String[] splited = data.split("\t");
            LinkedList<String> links = getLinks(splited[1]);
            String s = "L" + splited[0];
            for (String link: links) {
                if (link.charAt(0) == '/') {
                    context.write(new Text("http://lenta.ru" + link), new Text(s));
                } else {
                    context.write(new Text(link), new Text(s));
                }
            }
        }
    }

    static public class BuildGraphMapperUrls extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String[] splited = data.split("\t");
            context.write(new Text(splited[1]), new Text("U" + splited[0]));
        }
    }

    public static class BuildGraphReducer extends Reducer<Text, Text, IntWritable, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<Integer> set = new HashSet<>();
            int id = -1;
            for (Text v: values) {
                String s = v.toString();
                char code = s.charAt(0);
                if (code == 'U') {
                    id = Integer.parseInt(s.substring(1));
                } else {
                    set.add(Integer.parseInt(s.substring(1)));
                }
            }
            if (id == -1) {
                return;
            }
            for (Integer i: set) {
                context.write(new IntWritable(i), new IntWritable(id));
            }
        }
    }

    private Job getJobConfBuildGraph(String input, String urls, String outDir) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(BuildGraphJob.class);
        job.setJobName(BuildGraphJob.class.getCanonicalName());

        Path inputPath = new Path(input);
        Path urlsPath = new Path(urls);
        Path output = new Path(outDir);

        MultipleInputs.addInputPath(job, inputPath, TextInputFormat.class, BuildGraphMapperLinks.class);
        MultipleInputs.addInputPath(job, urlsPath, TextInputFormat.class, BuildGraphMapperUrls.class);
        FileOutputFormat.setOutputPath(job, output);

        job.setReducerClass(BuildGraphReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConfBuildGraph(args[0], args[1], args[2]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BuildGraphJob(), args);
        System.exit(exitCode);
    }
}
