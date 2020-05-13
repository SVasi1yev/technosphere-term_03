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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

public class CTRJob extends Configured implements Tool {
    private static final String URLS_FILENAME = "urls_filename";
    private static final String QUERIES_FILENAME = "queries_filename";
    private static final String GLOBAL_FEATURES_FILENAME = "globalFeatures";
    private static final String QUERY_TRAIN_FEATURES_FILENAME = "queryTrainFeatures";
    private static final String QUERY_TEST_FEATURES_FILENAME = "queryTestFeatures";
    private static final String TRAIN_SET_FILENAME = "train_set_filename";
    private static final String TEST_SET_FILENAME = "test_set_filename";

    public static class CTRMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private HashMap<String, Integer> urlsMap = new HashMap<>();
        private HashMap<String, Integer> queryMap = new HashMap<>();
        private HashSet<String> trainSet = new HashSet<>();
        private HashSet<String> testSet = new HashSet<>();

        private void makeMapFromFile(BufferedReader reader, HashMap<String, Integer> map) throws IOException {
            String line = reader.readLine();
            while (line != null){
                String[] splited = line.trim().split("\t");
                map.put(splited[1], Integer.parseInt(splited[0]));
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
            String line = reader.readLine();
            line = reader.readLine();
            while(line != null) {
                String[] splited = line.trim().split(",");
                set.add(splited[0] + " " + splited[1]);
                line = reader.readLine();
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
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

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sessionStr = value.toString();
            String query = new StringTokenizer(new StringTokenizer(sessionStr, "\t").nextToken(), "@").nextToken().trim();
            if (queryMap.containsKey(query)) {
                int queryId = queryMap.get(query);
                Session session = new Session(sessionStr);
                for (int i = 0; i < session.shown.length; i++){
                    String url = session.shown[i];
                    if (urlsMap.containsKey(url)) {
                        int urlId = urlsMap.get(url);
                        String pair = queryId + " " + urlId;
                        if (!trainSet.contains(pair) && !testSet.contains(pair)) {
                            StringBuilder res = new StringBuilder();
                            res.append("G");
                            res.append(" ");
                            res.append(i);
                            res.append(" ");
                            int clickIdx = -1;
                            for (int j = 0; j < session.clickedPos.length; j++) {
                                if (session.clickedPos[j] == i) {
                                    clickIdx = j;
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
                            context.write(new IntWritable(urlId), new Text(res.toString()));
                            continue;
                        }
                        StringBuilder res = new StringBuilder();
                        res.append(queryId); //queryID
                        res.append(" ");
                        res.append(i); //pos
                        res.append(" ");
                        res.append(session.minClickPos);
                        res.append(" ");
                        int clickIdx = -1;
                        for (int j = 0; j < session.clickedPos.length; j++) {
                            if (session.clickedPos[j] == i) {
                                clickIdx = j;
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
                        context.write(new IntWritable(urlId), new Text(res.toString()));
                    }
                }
            } else {
                Session session = new Session(sessionStr);
                for (int i = 0; i < session.shown.length; i++) {
                    String url = session.shown[i];
                    if (urlsMap.containsKey(url)) {
                        int urlId = urlsMap.get(url);
                        StringBuilder res = new StringBuilder();
                        res.append("G");
                        res.append(" ");
                        res.append(i);
                        res.append(" ");
                        int clickIdx = -1;
                        for (int j = 0; j < session.clickedPos.length; j++) {
                            if (session.clickedPos[j] == i) {
                                clickIdx = j;
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
                        context.write(new IntWritable(urlId), new Text(res.toString()));
                    }
                }
            }
        }
    }

    public static class CTRReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        class MutableLongs {
            private final double[] posBasedParams = {0.41, 0.16, 0.11, 0.08, 0.06,
                                                0.05, 0.04, 0.03, 0.025, 0.025};

            public long shows = 0;
            public long clicks = 0;

            public long[] posShows = new long[5];
            public long[] posClicks = new long[5];

            public double posBasedShows = 0;

            public long cascadeShows = 0;
            public long cascadeClicks = 0;

            public long dbnShows = 0;
            public long dbnClicks = 0;
            public long dbnSatisfied = 0;

            public long posSum = 0;

            public MutableLongs() {
                for (int i = 0; i < posShows.length; i++) {
                    posShows[i] = 0;
                }
                for (int i = 0; i < posClicks.length; i++) {
                    posClicks[i] = 0;
                }
            }

            public void incShows() {
                shows++;
            }

            public void incClicks() {
                clicks++;
            }

            public void incClicks(long i) {
                clicks += i;
            }

            public void incPos(byte pos, byte click) {
                if (pos < 5) {
                    posShows[pos]++;
                    posClicks[pos] += click;
                }
            }

//            public double countCtr() {
//                return (double) clicks / shows;
//            }

            public void incPosBasedShows(byte pos) {
                double param;
                if (pos < 10) {
                    param = posBasedParams[pos];
                } else {
                    param = posBasedParams[posBasedParams.length - 1];
                }
                posBasedShows += param;
            }

//            public double countPosBased() {
//                return (double) clicks / posBasedShows;
//            }

            public void incCascadeShows(byte pos, byte minClickPos) {
                if (pos <= minClickPos) {
                    cascadeShows++;
                }
            }

            public void incCascadeClicks(byte pos, byte minClickPos) {
                if (pos == minClickPos) {
                    cascadeClicks++;
                }
            }

            public void incDBN(byte pos, byte lastClickPos, byte click) {
                if (pos <= lastClickPos) {
                    dbnShows++;
                    dbnClicks += click;
                    if (pos == lastClickPos) {
                        dbnSatisfied++;
                    }
                }
            }

            public void incPosSum (byte pos) {
                posSum += pos;
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
            String line = reader.readLine();
            line = reader.readLine();
            while(line != null) {
                String[] splited = line.split(",");
                set.add(splited[0] + " " + splited[1]);
                line = reader.readLine();
            }
        }

        private MultipleOutputs<IntWritable, Text> out;
        private HashSet<String> trainSet = new HashSet<>();
        private HashSet<String> testSet = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("Reducer");

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

            out = new MultipleOutputs<IntWritable, Text>(context);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long clicks = 0;
            long shows = 0;
            long[] posShows = new long[5];
            for (int i = 0; i < posShows.length; i++) {
                posShows[i] = 0;
            }
            long[] posClicks = new long[5];
            for (int i = 0; i < posClicks.length; i++) {
                posClicks[i] = 0;
            }
            long satisfied = 0;
            long posSum = 0;
            HashMap<Integer, MutableLongs> queryStats = new HashMap<>();
            for (Text v: values) {
                String s = v.toString();
                if (s.charAt(0) == 'G') {
                    String[] splited = s.substring(2).split(" ");
                    byte pos = Byte.parseByte(splited[0]);
                    byte click = Byte.parseByte(splited[1]);
                    byte lastClickPos = Byte.parseByte(splited[2]);
                    shows++;
                    clicks += click;
                    if (pos < 5) {
                        posShows[pos]++;
                        posClicks[pos] += click;
                    }
                    if (pos == lastClickPos) {
                        satisfied++;
                    }
                    posSum += pos;
                    continue;
                }
                String[] splited = s.split(" ");
                int queryId = Integer.parseInt(splited[0]);
                byte pos = Byte.parseByte(splited[1]);
                byte minClickPos = Byte.parseByte(splited[2]);
                byte click = Byte.parseByte(splited[3]);
                byte lastClickPos = Byte.parseByte(splited[4]);
                shows++;
                clicks += click;
                if (pos < 5) {
                    posShows[pos]++;
                    posClicks[pos] += click;
                }
                MutableLongs t = queryStats.get(queryId);
                if (t == null) {
                    t = new MutableLongs();
                    t.incShows();
                    t.incClicks(click);
                    t.incPos(pos, click);
                    t.incPosBasedShows(pos);
                    t.incCascadeShows(pos, minClickPos);
                    t.incCascadeClicks(pos, minClickPos);
                    t.incDBN(pos, lastClickPos, click);
                    t.incPosSum(pos);
                    queryStats.put(queryId, t);
                } else {
                    t.incShows();
                    t.incClicks(click);
                    t.incPos(pos, click);
                    t.incPosBasedShows(pos);
                    t.incCascadeShows(pos, minClickPos);
                    t.incCascadeClicks(pos, minClickPos);
                    t.incDBN(pos, lastClickPos, click);
                    t.incPosSum(pos);
                }
            }

            StringBuilder globalRes = new StringBuilder();
            globalRes.append(shows); globalRes.append('\t');
            globalRes.append(clicks); globalRes.append('\t');
            for (int i = 0; i < posShows.length; i++) {
                globalRes.append(posShows[i]);
                globalRes.append('\t');
            }
            for (int i = 0; i < posClicks.length; i++) {
                globalRes.append(posClicks[i]);
                globalRes.append('\t');
            }
            globalRes.append(satisfied); globalRes.append('\t');
            globalRes.append((double) posSum / shows);
            out.write(GLOBAL_FEATURES_FILENAME, key, new Text(globalRes.toString()));

            for (Integer queryId: queryStats.keySet()) {
                StringBuilder queryRes = new StringBuilder();
                MutableLongs t = queryStats.get(queryId);
                queryRes.append(queryId); queryRes.append('\t');
                queryRes.append(t.shows); queryRes.append('\t');
                queryRes.append(t.clicks); queryRes.append('\t');
                for (int i = 0; i < t.posShows.length; i++) {
                    queryRes.append(t.posShows[i]);
                    queryRes.append('\t');
                }
                for (int i = 0; i < t.posClicks.length; i++) {
                    queryRes.append(t.posClicks[i]);
                    queryRes.append('\t');
                }
                queryRes.append(t.posBasedShows); queryRes.append('\t');
                queryRes.append(t.cascadeShows); queryRes.append('\t');
                queryRes.append(t.cascadeClicks); queryRes.append('\t');
                queryRes.append(t.dbnShows); queryRes.append('\t');
                queryRes.append(t.dbnClicks); queryRes.append('\t');
                queryRes.append(t.dbnSatisfied); queryRes.append('\t');
                queryRes.append((double) t.posSum / t.shows);
                String pair = queryId + " " + key.get();
                if (trainSet.contains(pair)) {
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

    private Job getCTRJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(CTRJob.class);
        job.setJobName(CTRJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleOutputs.addNamedOutput(job, GLOBAL_FEATURES_FILENAME, TextOutputFormat.class, IntWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QUERY_TRAIN_FEATURES_FILENAME, TextOutputFormat.class, IntWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QUERY_TEST_FEATURES_FILENAME, TextOutputFormat.class, IntWritable.class, Text.class);

        job.setMapperClass(CTRMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(CTRReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getCTRJobConf(args[0], args[1]);
        job.getConfiguration().set(URLS_FILENAME, args[2]);
        job.getConfiguration().set(QUERIES_FILENAME, args[3]);
        job.getConfiguration().set(TRAIN_SET_FILENAME, args[4]);
        job.getConfiguration().set(TEST_SET_FILENAME, args[5]);
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
        int exitCode = ToolRunner.run(new CTRJob(), args);
        long end = System.currentTimeMillis();
        System.out.println("Time: " + (double) (end - start) / 1000);
        System.exit(exitCode);
    }
}
