import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class CTRJob extends Configured implements Tool {
    private static final String URLS_FILENAME = "urls_ilename";
    private static final String GLOBAL_CTR_FILENAME = "globalCtr";
    private static final String POS_CTR_FILENAME = "posCtr";
    private static final String QUERY_CTR_FILENAME = "queryCtr";

    public static class CTRMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private HashMap<String, Integer> urlsMap = new HashMap<>();
        int j = 1;

        private void makeMapFromFile(BufferedReader reader, HashMap<String, Integer> map) throws IOException {
            String line = reader.readLine();
            while (line != null){
                String[] splited = line.split("\t");
                map.put(splited[1], Integer.parseInt(splited[0]));
                line=reader.readLine();
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String urlsFileName = context.getConfiguration().get(URLS_FILENAME);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader urlsFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(urlsFileName)), StandardCharsets.UTF_8));
            makeMapFromFile(urlsFileReader, urlsMap);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Session session = new Session(value.toString(), urlsMap);
            System.out.println(j + " " + session.query);
            j += 1;
            for (int i = 0; i < session.shown.size(); i++) {
                StringBuilder res = new StringBuilder();
                res.append(session.query);
                res.append("\t");
                String url = session.shown.get(i);
                if (session.clicked.contains(url)) {
                    res.append(1);
                } else {
                    res.append(0);
                }
                res.append("\t");
                res.append(session.shownPositions.get(i));
                context.write(new IntWritable(urlsMap.get(url)), new Text(res.toString()));
            }
        }
    }

    public static class CTRReducer extends Reducer<IntWritable, Text, Text, Text> {
        private HashMap<Integer, String> urlsMap = new HashMap<>();
        private MultipleOutputs<Text, Text> out;

        private void makeMapFromFile(BufferedReader reader, HashMap<Integer, String> map) throws IOException {
            String line = reader.readLine();
            while (line != null){
                String[] splited = line.split("\t");
                map.put(Integer.parseInt(splited[0]), splited[1]);
                line=reader.readLine();
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String urlsFileName = context.getConfiguration().get(URLS_FILENAME);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader urlsFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(urlsFileName)), StandardCharsets.UTF_8));
            makeMapFromFile(urlsFileReader, urlsMap);
            out = new MultipleOutputs<Text, Text>(context);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long clicks = 0;
            long shows = 0;
            HashMap<String, Long> queryClicks = new HashMap<>();
            HashMap<String, Long> queryShows = new HashMap<>();
            HashMap<Integer, Long> posClicks = new HashMap<>();
            HashMap<Integer, Long> posShows = new HashMap<>();

            for (Text v: values) {
                String s = v.toString();
                System.out.println(s);
                String[] splited = s.split("\t");
                String query = splited[0];
                boolean click = splited[1].equals("1");
                int pos = Integer.parseInt(splited[2]);
                shows++;
                queryShows.put(query, queryShows.getOrDefault(query, 0L) + 1);
                posShows.put(pos, posShows.getOrDefault(pos, 0L) + 1);
                if (click) {
                    clicks++;
                    queryClicks.put(query, queryClicks.getOrDefault(query, 0L) + 1);
                    posClicks.put(pos, posClicks.getOrDefault(pos, 0L) + 1);
                }
            }
            String url = urlsMap.get(key.get());
            System.out.println(url);
            out.write(GLOBAL_CTR_FILENAME, new Text(url), new Text(Double.toString((double) clicks / shows)));
            for (Integer pos: posShows.keySet()) {
                out.write(POS_CTR_FILENAME, new Text(url), new Text(pos + " " + (double) posClicks.getOrDefault(pos, 0L) / posShows.get(pos)));
            }
            for (String query: queryShows.keySet()) {
                out.write(QUERY_CTR_FILENAME, new Text(url), new Text(query + " " + (double) queryClicks.getOrDefault(query, 0L) / queryShows.get(query)));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            out.close();
        }
    }

    private Job getCTRJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(CTRJob.class);
        job.setJobName(CTRJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleOutputs.addNamedOutput(job, GLOBAL_CTR_FILENAME, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, POS_CTR_FILENAME, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QUERY_CTR_FILENAME, TextOutputFormat.class, Text.class, Text.class);

        job.setMapperClass(CTRMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(CTRReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(8);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getCTRJobConf(args[0], args[1]);
        job.getConfiguration().set(URLS_FILENAME, args[2]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public static void main(String[] args) throws Exception {
        deleteDirectory(new File(args[1]));
        int exitCode = ToolRunner.run(new CTRJob(), args);
        System.exit(exitCode);
    }
}
