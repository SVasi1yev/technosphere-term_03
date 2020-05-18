import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByteText implements WritableComparable<ByteText> {
    public ByteWritable isRobots;
    public Text host;

    public ByteText() {
        isRobots = new ByteWritable();
        host = new Text();
    }
    public ByteText(ByteWritable isRobots, Text host) {
        this.isRobots = isRobots;
        this.host = host;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        isRobots.write(out);
        host.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        isRobots.readFields(in);
        host.readFields(in);
    }

    @Override
    public int hashCode() {
        return host.hashCode();
    }

    @Override
    public int compareTo(ByteText o) {
        int res = host.compareTo(o.host);
        if (res == 0) {
            if (isRobots.get() == o.isRobots.get()) {
                return 0;
            }
            if (isRobots.get() == 1) {
                return -1;
            } else {
                return 1;
            }
        }
        return res;
    }

    public static class ByteTextComparator extends WritableComparator {
        public ByteTextComparator() {
            super(ByteText.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((ByteText) a).compareTo((ByteText) b);
        }
    }

    public static class ByteTextGroupComparator extends WritableComparator {
        public ByteTextGroupComparator() {
            super(ByteText.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((ByteText) a).host.toString().compareTo(((ByteText) b).host.toString());
        }
    }

    public static class ByteTextPartitioner extends Partitioner<ByteText, Text> {
        @Override
        public int getPartition(ByteText byteText, Text text, int numPartitions) {
            return Math.abs(byteText.hashCode()) % numPartitions;
        }
    }
}
