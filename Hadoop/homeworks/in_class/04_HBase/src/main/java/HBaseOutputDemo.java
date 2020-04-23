import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import static java.nio.charset.StandardCharsets.*;

import java.io.IOException;


public class HBaseOutputDemo extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(args);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    Job GetJobConf(String[] args) throws IOException {
        String input_path = args[0];
        String output_table = args[1];

        Job job = Job.getInstance(getConf(), "HBaseOutputDemo");
        job.setJarByClass(HBaseWordCount.class);
        FileInputFormat.addInputPath(job, new Path(input_path));

        job.setMapperClass(TextInputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.initTableReducerJob(output_table,
                DocWriteReducer.class,
                job);

        job.setNumReduceTasks(4);

        return job;
    }

    static public class TextInputMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\t");
            if (items.length != 2) {
                context.getCounter("COMMON", "bad_input").increment(1);
                return;
            }

            String url = items[0];
            String doc = new String(Base64.decodeBase64(items[1]), UTF_8);

            context.write(new Text(url), new Text(doc));
        }
    }

    static public class DocWriteReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> texts, Context context) throws IOException, InterruptedException {
            String url = key.toString();
            String text = texts.iterator().next().toString();

            Put put = new Put(url.getBytes());
            put.addColumn(Bytes.toBytes("htmls"), Bytes.toBytes("text"), text.getBytes(UTF_8));

            context.write(new ImmutableBytesWritable(url.getBytes()), put);
        }
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(HBaseConfiguration.create(), new HBaseOutputDemo(), args);
        System.exit(rc);
    }
}
