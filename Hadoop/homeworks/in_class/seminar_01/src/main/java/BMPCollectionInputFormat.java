import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Read Bitmaps written one after another
 * The thing is - bitmaps of same dimension has same size in bytes.
 * Their size is: width * height * 3 (bytes) + 16 (header size)
 */
public class BMPCollectionInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

    public class BMPRecordReader extends RecordReader<NullWritable, BytesWritable> {
        FSDataInputStream input;
        long nimages;
        long cur_img = 0;
        BytesWritable value = new BytesWritable();

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSplit fsplit = (FileSplit)split;
            Path path = fsplit.getPath();
            FileSystem fs = path.getFileSystem(conf);

            input = fs.open(path);
            input.seek(fsplit.getStart());
            long img_size = getNumBytesPerImage(conf);
            nimages = fsplit.getLength() / img_size;

            value.setCapacity((int)img_size);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (cur_img >= nimages)
                return false;

            IOUtils.readFully(input, value.getBytes(), 0, value.getCapacity());
            cur_img++;
            return true;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float)cur_img / nimages;
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeStream(input);
        }
    }

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        BMPRecordReader reader = new BMPRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>();

        for (FileStatus status: listStatus(context)) {
            long split_size = getNumBytesPerSplit(context.getConfiguration());
            long flen = status.getLen();
            Path path = status.getPath();

            for(int i = 0; i * split_size < flen; ++i) {
                splits.add(new FileSplit(path, i * split_size, ((i + 1) * split_size < flen) ? split_size : flen - i * split_size, null));
            }
        }

        return splits;
    }

    public static final String BYTES_PER_IMAGE = "mapreduce.input.bmp.bytes_per_image";
    public static final String BYTES_PER_MAP = "mapreduce.input.bmp.bytes_per_map";

    public static long getNumBytesPerSplit(Configuration conf) {
        long split_size = conf.getLong(BYTES_PER_MAP, 134217728);
        long img_size = getNumBytesPerImage(conf);

        return split_size / img_size * img_size;
    }

    public static long getNumBytesPerImage(Configuration conf) {
        return conf.getLong(BYTES_PER_IMAGE, 537654);
    }
}
