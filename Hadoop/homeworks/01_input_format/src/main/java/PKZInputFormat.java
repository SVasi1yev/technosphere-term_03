import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class PKZInputFormat extends FileInputFormat<LongWritable, Text> {
    public static final long MAX_SPLIT_SIZE = 67_108_864;

    public class PZKInsputSplit extends InputSplit {
        Path dataPath;
        long dataStart;
        long dataLength;

        Path idxPath;
        long idxStart;
        long idxLength;

        long globalOffset;

        public PZKInsputSplit(Path dataPath, long dataStart, long dataLength, Path idxPath, long idxStart, long idxLength, long globalOffset) {
            this.dataPath = dataPath;
            this.dataStart = dataStart;
            this.dataLength = dataLength;
            this.idxPath = idxPath;
            this.idxStart = idxStart;
            this.idxLength = idxLength;
            this.globalOffset = globalOffset;
        }

        @Override
        public long getLength() {
            return dataLength + idxLength;
        }

        @Override
        public String[] getLocations() {
            return null;
        }

        public Path getDataPath() {
            return dataPath;
        }

        public long getDataStart() {
            return dataStart;
        }

        public long getDataLength() {
            return dataLength;
        }

        public Path getIdxPath() {
            return idxPath;
        }

        public long getIdxStart() {
            return idxStart;
        }

        public long getIdxLength() {
            return idxLength;
        }

        public long getGlobalOffset() {
            return globalOffset;
        }
    }

    public class PZKRecordReader extends RecordReader<LongWritable, Text> {
        FSDataInputStream dataInput;
        FSDataInputStream idxInput;
        long recordsNum;
        long curRecord = 0;
        LongWritable offset;
        int lastRecortLength = 0;
        Text document;
        Inflater decoder;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            PZKInsputSplit fsplit = (PZKInsputSplit) split;
            Path dataPath = fsplit.getDataPath();
            Path idxPath = fsplit.getIdxPath();
            FileSystem dataFs = dataPath.getFileSystem(conf);
            FileSystem idxFs = idxPath.getFileSystem(conf);

            dataInput = dataFs.open(dataPath);
            dataInput.seek(fsplit.getDataStart());
            idxInput = idxFs.open(idxPath);
            idxInput.seek(fsplit.getIdxStart());

            recordsNum = fsplit.getIdxLength() / 4;
            offset = new LongWritable(fsplit.getGlobalOffset());
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (curRecord >= recordsNum) {
                return false;
            }
            offset.set(offset.get() + lastRecortLength);
            lastRecortLength = idxInput.readInt();
            byte[] data = new byte[lastRecortLength];
            IOUtils.readFully(dataInput, data, 0, lastRecortLength);
            decoder.setInput(data);
            try {
                decoder.inflate(document.getBytes());
            } catch (DataFormatException e) {
                System.out.println("Invalid decompression at offset " + offset.toString());
            }
            return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return offset;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return document;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return  (float) curRecord / recordsNum;
        }

        @Override
        public void close() throws  IOException {
            IOUtils.closeStream(dataInput);
            IOUtils.closeStream(idxInput);
        }
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader (InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
        PZKRecordReader recordReader = new PZKRecordReader();
        recordReader.initialize(split, context);
        return recordReader;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>();
        FSDataInputStream idxInput;
        Configuration conf = context.getConfiguration();
        long globalOffset = 0;

        for (FileStatus dataStatus: listStatus(context)) {
            Path dataPath = dataStatus.getPath();
            Path idxPath = new Path(dataPath.getParent(), dataPath.getName() + ".idx");
            FileSystem fs = idxPath.getFileSystem(conf);
            idxInput = fs.open(idxPath);

            long dataLength = dataStatus.getLen();
            long procData = 0;
            int recordLength;
            long splitDataStart = 0;
            long splitDataLength = 0;
            long splitIdxStart = 0;
            long splitIdxLength = 0;
            while (procData < dataLength) {
                recordLength = idxInput.readInt();
                if ((splitDataLength + recordLength < MAX_SPLIT_SIZE) || (splitDataLength == 0)) {
                    splitDataLength += recordLength;
                    splitIdxLength += 4;
                } else {
                    splits.add(
                        new PZKInsputSplit(
                            dataPath, splitDataStart, splitDataLength,
                            idxPath, splitIdxStart, splitIdxLength,
                            globalOffset
                        )
                    );
                    globalOffset += splitDataLength;
                    splitDataStart += splitDataLength;
                    splitIdxStart += splitIdxLength;
                    splitDataLength = recordLength;
                    splitIdxLength = 4;
                }
                procData += recordLength;
            }
            if (splitDataLength > 0) {
                splits.add(
                    new PZKInsputSplit(
                        dataPath, splitDataStart, splitDataLength,
                        idxPath, splitIdxStart, splitIdxLength,
                        globalOffset
                    )
                );
                globalOffset += splitDataLength;
            }
        }

        return splits;
    }
}
