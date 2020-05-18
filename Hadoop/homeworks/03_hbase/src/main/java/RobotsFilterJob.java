import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class RobotsFilterJob extends Configured implements Tool {
    private static final String WEBPAGES_TABLE = "webpages_table";
    private static final String WEBSITES_TABLE = "websites_table";
    static final String DISABLE_LABEL = "D";
    static final String ENABLE_LABEL = "E";

    public static class RobotsFilterMapper extends TableMapper<ByteText, Text> {
        String webPagesTable;
        String webSitesTable;
        ByteWritable zero = new ByteWritable((byte) 0);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            webPagesTable = context.getConfiguration().get(WEBPAGES_TABLE);
            webSitesTable = context.getConfiguration().get(WEBSITES_TABLE);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            TableSplit curSplit = (TableSplit) context.getInputSplit();
            String tableName = new String(curSplit.getTableName());
            if (tableName.equals(webSitesTable)) {
                String host = new String(value.getValue(Bytes.toBytes("info"), Bytes.toBytes("site")));
                byte[] bytes = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("robots"));
                String robots;
                if (bytes == null) {
                    robots = "";
                } else {
                    robots = new String(bytes);
                }
                context.write(new ByteText(new ByteWritable((byte) 1), new Text(host)), new Text(robots));
            } else if (tableName.equals(webPagesTable)) {
                String urlStr = new String(value.getValue(Bytes.toBytes("docs"), Bytes.toBytes("url")));
                URL url = new URL(urlStr);
                boolean isEnable = value.getValue(Bytes.toBytes("docs"), Bytes.toBytes("disabled")) == null;
                String res;
                if (isEnable) {
                    res = ENABLE_LABEL + urlStr;
                } else {
                    res = DISABLE_LABEL + urlStr;
                }
                context.write(new ByteText(zero, new Text(url.getHost())), new Text(res));
            }
        }
    }

    public static class RobotsFilterReducer extends TableReducer<ByteText, Text, ImmutableBytesWritable> {
        private byte[] MD5(String string) throws InterruptedException {
            MessageDigest md;
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new InterruptedException(e.getMessage());
            }
            return DatatypeConverter.printHexBinary(md.digest(string.getBytes())).toLowerCase().getBytes();
        }

        @Override
        protected void reduce(ByteText key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            RobotsFilter filter = null;

            for (Text v: values) {
                String value = v.toString();

                if (key.isRobots.get() == 1) {
                    try {
                        filter = new RobotsFilter(value);
                    } catch (RobotsFilter.BadFormatException e) {
                        throw new InterruptedException(e.getMessage());
                    }
                } else {
                    boolean wasDisabled = value.startsWith(DISABLE_LABEL);
                    String url = value.substring(DISABLE_LABEL.length());
                    assert filter != null;
                    boolean isDisabled = filter.IsDisable(url);
                    byte[] hash = MD5(url);

                    if (wasDisabled && !isDisabled) {
                        Delete del = new Delete(hash);
                        del.deleteColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
                        context.write(null, del);
                    } else if (!wasDisabled && isDisabled) {
                        Put put = new Put(hash);
                        put.add(Bytes.toBytes("docs"), Bytes.toBytes("disabled"), Bytes.toBytes("Y"));
                        context.write(null, put);
                    }
                }
            }
        }
    }

    private Job getJobConf(String webPagesTable, String webSitesTable) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(RobotsFilterJob.class);
        job.setJobName(RobotsFilterJob.class.getCanonicalName());

        Configuration conf = job.getConfiguration();
        conf.set(WEBPAGES_TABLE, webPagesTable);
        conf.set(WEBSITES_TABLE, webSitesTable);

        List<Scan> scans = new ArrayList<Scan>();

        Scan webPagesScan = new Scan();
        webPagesScan.setAttribute("scan.attributes.table.name", Bytes.toBytes(webPagesTable));
        scans.add(webPagesScan);

        Scan webSitesScan = new Scan();
        webSitesScan.setAttribute("scan.attributes.table.name", Bytes.toBytes(webSitesTable));
        scans.add(webSitesScan);

        TableMapReduceUtil.initTableMapperJob(scans, RobotsFilterMapper.class, ByteText.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(webPagesTable, RobotsFilterReducer.class, job);

        job.setNumReduceTasks(2);

        job.setGroupingComparatorClass(ByteText.ByteTextGroupComparator.class);
        job.setSortComparatorClass(ByteText.ByteTextComparator.class);
        job.setPartitionerClass(ByteText.ByteTextPartitioner.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RobotsFilterJob(), args);
        System.exit(exitCode);
    }
}
