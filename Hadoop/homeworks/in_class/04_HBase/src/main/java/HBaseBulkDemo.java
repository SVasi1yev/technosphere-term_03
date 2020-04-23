import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
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


public class HBaseBulkDemo extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(HBaseConfiguration.create(), new HBaseBulkDemo(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        String input_path = args[0];
        TableName output_table = TableName.valueOf(args[1]);
        BulkLoadHelper load_helper = new BulkLoadHelper(getConf());

        Job job = MakeJobConf(input_path, output_table, load_helper.GetBulksDirectory());
        if (!job.waitForCompletion(true))
            return 1;

        load_helper.BulkLoad(output_table);
        return 0;
    }

    private Job MakeJobConf(String input_path, TableName output_table, Path bulks_dir) throws IOException {
        Job job = Job.getInstance(getConf(), HBaseBulkDemo.class.getName());
        job.setJarByClass(HBaseBulkDemo.class);
        FileInputFormat.addInputPath(job, new Path(input_path));

        job.setMapperClass(TextInputMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        Connection connection = ConnectionFactory.createConnection(getConf());
        HTableDescriptor tbl = new HTableDescriptor(output_table);
        HFileOutputFormat2.configureIncrementalLoad(job, tbl, connection.getRegionLocator(output_table));
        HFileOutputFormat2.setOutputPath(job, bulks_dir);

        return job;
    }

    static public class TextInputMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\t");
            if (items.length != 2) {
                context.getCounter("COMMON", "bad_input").increment(1);
                return;
            }

            String url = items[0];
            String doc = new String(Base64.decodeBase64(items[1]), UTF_8);

            Put put = new Put(url.getBytes());
            put.addColumn(Bytes.toBytes("htmls"), Bytes.toBytes("text"), doc.getBytes(UTF_8))
                    .addColumn(Bytes.toBytes("htmls"), Bytes.toBytes("size"), Bytes.toBytes(doc.length()));

            context.write(new ImmutableBytesWritable(url.getBytes()), put);
        }
    }
}
