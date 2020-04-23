import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HostCount implements WritableComparable<HostCount> {
    private Text host;
    private LongWritable count;

    public HostCount() {
        this.host = new Text();
        this.count = new LongWritable();
    }

    public HostCount(Text host, LongWritable count) {
        this.host = host;
        this.count = count;
    }

    public Text getHost() {
        return host;
    }

    public LongWritable getCount() {
        return count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        host.readFields(in);
        count.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        host.write(out);
        count.write(out);
    }

    @Override
    public int compareTo(HostCount o) {
        int compareHosts = this.host.compareTo(o.host);
        return (compareHosts != 0) ? compareHosts : -this.count.compareTo(o.count);
    }

    @Override
    public int hashCode() {
        return host.hashCode();
    }

    @Override
    public String toString() {
        return "HostCount{" +
                "host=" + host +
                ", count=" + count +
                '}';
    }
}
