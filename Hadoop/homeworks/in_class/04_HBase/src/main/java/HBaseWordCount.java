import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static java.nio.charset.StandardCharsets.*;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HBaseWordCount extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(args);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    Job GetJobConf(String[] args) throws IOException {
        String input_table = args[0];
        String output_path = args[1];

        Job job = Job.getInstance(getConf(), HBaseWordCount.class.getCanonicalName());
        job.setJarByClass(HBaseWordCount.class);

        Scan scan = new Scan().addColumn(Bytes.toBytes("htmls"), Bytes.toBytes("text")).
			       addColumn(Bytes.toBytes("htmls"), Bytes.toBytes("size"));
        /* write your filter expression here */

        TableMapReduceUtil.initTableMapperJob(
                input_table,
                scan,
                DemoMapper.class,
                Text.class, IntWritable.class,
                job);

        job.setCombinerClass(DemoReducer.class);
        job.setReducerClass(DemoReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(output_path));

        return job;
    }

    static public class DemoMapper extends TableMapper<Text, IntWritable> {
        static IntWritable one = new IntWritable(1);
        static final Pattern word_expr = Pattern.compile("\\p{L}+");

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Cell cell = value.getColumnLatestCell(Bytes.toBytes("htmls"), Bytes.toBytes("text"));
            String text = new String(CellUtil.cloneValue(cell), UTF_8);

            Cell cell_size = value.getColumnLatestCell(Bytes.toBytes("htmls"), Bytes.toBytes("size"));
            int n = Bytes.toInt(CellUtil.cloneValue(cell_size));
            if (n < 9*1024 || n > 10*1024)
                context.getCounter("COMMON", "BAD_REDUCER_INPUTS").increment(1);

            Matcher matcher = word_expr.matcher(text);
            while (matcher.find()) {
                String word = matcher.group().toLowerCase();
                context.write(new Text(word), one);
            }
        }
    }

    static public class DemoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable x: values) {
                sum += x.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(HBaseConfiguration.create(), new HBaseWordCount(), args);
        System.exit(rc);
    }
}
