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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

public class DocDocQueryStatsJob extends Configured implements Tool {
    private static final String URLS_FILENAME = "urls_filename";
    private static final String QUERIES_FILENAME = "queries_filename";
    private static final String GLOBAL_FEATURES_FILENAME = "globalFeatures";
    private static final String QUERY_TRAIN_FEATURES_FILENAME = "queryTrainFeatures";
    private static final String QUERY_TEST_FEATURES_FILENAME = "queryTestFeatures";
    private static final String TRAIN_SET_FILENAME = "train_set_filename";
    private static final String TEST_SET_FILENAME = "test_set_filename";
    private static final String FEATURES_TYPE = "features_type";

    public static class DocDocQueryStatsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final HashMap<String, Integer> urlsMap = new HashMap<>();
        private final HashMap<String, Integer> queryMap = new HashMap<>();
        private final HashSet<String> trainSet = new HashSet<>();
        private final HashSet<String> testSet = new HashSet<>();
        private boolean hostFeaturestype = false;

        private void makeMapFromFile(BufferedReader reader, HashMap<String, Integer> map) throws IOException {
            String line = reader.readLine();
            while (line != null){
                String[] splited = line.trim().split("\t");
                Integer id = Integer.parseInt(splited[0]);
                String url = splited[1];
                url = url.replace("http://","").replace("https://","").replace("www.", "");
                if (url.endsWith("/")) {
                    url = url.substring(0, url.length() - 1);
                }
                map.put(url, id);
                line = reader.readLine();
            }
        }

        private void readTrainSet(BufferedReader reader, HashSet<String> set) throws IOException {
            String line = reader.readLine();
            while (line != null) {
                String[] splited = line.trim().split("\t");
                set.add(splited[0] + " " + splited[1]);
                line = reader.readLine();
            }
        }

        private void readTestSet(BufferedReader reader, HashSet<String> set) throws IOException {
            reader.readLine();
            String line = reader.readLine();
            while(line != null) {
                String[] splited = line.trim().split(",");
                set.add(splited[0] + " " + splited[1]);
                line = reader.readLine();
            }
        }

        @Override
        protected void setup(Context context) throws IOException {
            hostFeaturestype = context.getConfiguration().get(FEATURES_TYPE).equals("host");

            String urlsFileName = context.getConfiguration().get(URLS_FILENAME);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader urlsFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(urlsFileName)), StandardCharsets.UTF_8));
            long start = System.currentTimeMillis();
            makeMapFromFile(urlsFileReader, urlsMap);
            long end = System.currentTimeMillis();
            urlsFileReader.close();
            System.out.println("Time to read urlMap in mapper: " + (double) (end - start) / 1000);

            String queriesFileName = context.getConfiguration().get(QUERIES_FILENAME);
            BufferedReader queriesFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(queriesFileName)), StandardCharsets.UTF_8));
            start = System.currentTimeMillis();
            makeMapFromFile(queriesFileReader, queryMap);
            end = System.currentTimeMillis();
            queriesFileReader.close();
            System.out.println("Time to read queriesMap in mapper: " + (double) (end - start) / 1000);

            String trainSetFileName = context.getConfiguration().get(TRAIN_SET_FILENAME);
            BufferedReader trainSetFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(trainSetFileName)), StandardCharsets.UTF_8));
            start = System.currentTimeMillis();
            readTrainSet(trainSetFileReader, trainSet);
            end = System.currentTimeMillis();
            trainSetFileReader.close();
            System.out.println("Time to read trainSet in mapper: " + (double) (end - start) / 1000);

            String testSetFileName = context.getConfiguration().get(TEST_SET_FILENAME);
            BufferedReader testSetFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(testSetFileName)), StandardCharsets.UTF_8));
            start = System.currentTimeMillis();
            readTestSet(testSetFileReader, testSet);
            end = System.currentTimeMillis();
            testSetFileReader.close();
            System.out.println("Time to read testSet in mapper: " + (double) (end - start) / 1000);
        }

        private StringBuilder makeValue(Session session, int pos, String prefix) {
            StringBuilder res = new StringBuilder();
            res.append(prefix); res.append(" ");
            res.append(pos); res.append(" ");
            res.append(session.minClickPos); res.append(" ");
            int clickIdx = -1;
            for (int i = 0; i < session.clickedPos.length; i++) {
                if (session.clickedPos[i] == pos) {
                    clickIdx = i;
                    break;
                }
            }
            if (clickIdx == -1) {
                res.append(0);
            } else {
                res.append(1);
            }
            res.append(" ");
            res.append(session.clickedPos[session.clickedPos.length - 1]);
            return res;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sessionStr = value.toString();
            String query = new StringTokenizer(new StringTokenizer(sessionStr, "\t").nextToken(), "@").nextToken().trim();
            Session session = new Session(sessionStr);
            if (queryMap.containsKey(query)) {
                int queryId = queryMap.get(query);
                for (int i = 0; i < session.shown.length; i++){
                    String url = session.shown[i];
                    if (hostFeaturestype) {
                        try {
                            url = new URL("http://" + url).getHost();
                        } catch (Exception e) {
//                            System.out.println(e.getMessage());
                            continue;
                        }
                    }
                    if (urlsMap.containsKey(url)) {
                        int urlId = urlsMap.get(url);
                        String pair = queryId + " " + urlId;
                        StringBuilder res;
                        if (!trainSet.contains(pair) && !testSet.contains(pair)) {
                            res = makeValue(session, i, "G");
                        } else {
                            res = makeValue(session, i, Integer.toString(queryId));
                        }
                        context.write(new IntWritable(urlId), new Text(res.toString()));
                    }
                }
            } else {
                for (int i = 0; i < session.shown.length; i++) {
                    String url = session.shown[i];
                    if (hostFeaturestype) {
                        try {
                            url = new URL("http://" + url).getHost();
                        } catch (Exception e) {
//                            System.out.println(e.getMessage());
                            continue;
                        }
                    }
                    if (urlsMap.containsKey(url)) {
                        int urlId = urlsMap.get(url);
                        StringBuilder res = makeValue(session, i, "G");
                        context.write(new IntWritable(urlId), new Text(res.toString()));
                    }
                }
            }
        }
    }

    public static class DocDocQueryStatsReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private void readTrainSet(BufferedReader reader, HashSet<String> set) throws IOException {
            String line = reader.readLine();
            while (line != null) {
                String[] splited = line.trim().split("\t");
                set.add(splited[0] + " " + splited[1]);
                line = reader.readLine();
            }
        }

        private void readTestSet(BufferedReader reader, HashSet<String> set) throws IOException {
            reader.readLine();
            String line = reader.readLine();
            while(line != null) {
                String[] splited = line.split(",");
                set.add(splited[0] + " " + splited[1]);
                line = reader.readLine();
            }
        }

        private MultipleOutputs<IntWritable, Text> out;
        private final HashSet<String> trainSet = new HashSet<>();
        private final HashSet<String> testSet = new HashSet<>();
        private boolean hostFeaturestype = false;

        @Override
        protected void setup(Context context) throws IOException {
            System.out.println("Reducer");

            hostFeaturestype = context.getConfiguration().get(FEATURES_TYPE).equals("host");

            String trainSetFileName = context.getConfiguration().get(TRAIN_SET_FILENAME);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader trainSetFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(trainSetFileName)), StandardCharsets.UTF_8));
            long start = System.currentTimeMillis();
            readTrainSet(trainSetFileReader, trainSet);
            long end = System.currentTimeMillis();
            trainSetFileReader.close();
            System.out.println("Time to read trainSet in reducer: " + (double) (end - start) / 1000);

            String testSetFileName = context.getConfiguration().get(TEST_SET_FILENAME);
            BufferedReader testSetFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(testSetFileName)), StandardCharsets.UTF_8));
            start = System.currentTimeMillis();
            readTestSet(testSetFileReader, testSet);
            end = System.currentTimeMillis();
            testSetFileReader.close();
            System.out.println("Time to read testSet in reducer: " + (double) (end - start) / 1000);

            out = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            MutableLongs globalStats = new MutableLongs();
            HashMap<Integer, MutableLongs> queryStats = new HashMap<>();
            for (Text v: values) {
                String s = v.toString();
                boolean glob = false;
                if (s.charAt(0) == 'G') {
                    glob = true;
                }

                String[] splited = s.split(" ");
                int queryId = -1;
                if (!glob) {
                    queryId = Integer.parseInt(splited[0]);
                }
                int pos = Integer.parseInt(splited[1]);
                int minClickPos = Integer.parseInt(splited[2]);
                byte click = Byte.parseByte(splited[3]);
                int lastClickPos = Integer.parseInt(splited[4]);

                globalStats.updateAll(click, pos, minClickPos, lastClickPos);
                if (!glob) {
                    MutableLongs t = queryStats.get(queryId);
                    if (t == null) {
                        t = new MutableLongs();
                        t.updateAll(click, pos, minClickPos, lastClickPos);
                        queryStats.put(queryId, t);
                    } else {
                        t.updateAll(click, pos, minClickPos, lastClickPos);
                    }
                }
            }

            StringBuilder globalRes = globalStats.makeValue(-1);
            out.write(GLOBAL_FEATURES_FILENAME, key, new Text(globalRes.toString()));

            for (Integer queryId: queryStats.keySet()) {
                MutableLongs t = queryStats.get(queryId);
                StringBuilder queryRes = t.makeValue(queryId);
                String pair = queryId + " " + key.get();
                if (trainSet.contains(pair) || hostFeaturestype) {
                    out.write(QUERY_TRAIN_FEATURES_FILENAME, key, new Text(queryRes.toString()));
                } else if (testSet.contains(pair)) {
                    out.write(QUERY_TEST_FEATURES_FILENAME, key, new Text(queryRes.toString()));
                } else {
                    System.out.println("ERROR IN REDUCER");
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            out.close();
        }
    }

    private Job getDocDocQueryStatsJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(DocDocQueryStatsJob.class);
        job.setJobName(DocDocQueryStatsJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleOutputs.addNamedOutput(job, GLOBAL_FEATURES_FILENAME, TextOutputFormat.class, IntWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QUERY_TRAIN_FEATURES_FILENAME, TextOutputFormat.class, IntWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QUERY_TEST_FEATURES_FILENAME, TextOutputFormat.class, IntWritable.class, Text.class);

        job.setMapperClass(DocDocQueryStatsMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DocDocQueryStatsReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getDocDocQueryStatsJobConf(args[0], args[1]);
        job.getConfiguration().set(URLS_FILENAME, args[2]);
        job.getConfiguration().set(QUERIES_FILENAME, args[3]);
        job.getConfiguration().set(TRAIN_SET_FILENAME, args[4]);
        job.getConfiguration().set(TEST_SET_FILENAME, args[5]);
        job.getConfiguration().set(FEATURES_TYPE, args[6]);
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
        long start = System.currentTimeMillis();
        int exitCode = ToolRunner.run(new DocDocQueryStatsJob(), args);
        long end = System.currentTimeMillis();
        System.out.println("Time: " + (double) (end - start) / 1000);
        System.exit(exitCode);
    }
}
