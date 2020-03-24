import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class PKZInputFormat extends FileInputFormat<LongWritable, Text> {
    public static class PZKInputSplit extends InputSplit implements Writable {
        Path dataPath;
        Text dataTextPath;
        LongWritable dataStart;
        LongWritable dataLength;

        Path idxPath;
        Text idxTextPath;
        LongWritable idxStart;
        LongWritable idxLength;

        LongWritable globalOffset;

        public PZKInputSplit() {
            this.dataTextPath = new Text();
            this.dataStart = new LongWritable();
            this.dataLength = new LongWritable();

            this.idxTextPath = new Text();
            this.idxStart = new LongWritable();
            this.idxLength = new LongWritable();
            this.globalOffset = new LongWritable();
        }

        public PZKInputSplit(Path dataPath, long dataStart, long dataLength, Path idxPath, long idxStart, long idxLength, long globalOffset) {
            this.dataPath = dataPath;
            this.dataTextPath = new Text(this.dataPath.toString());
            this.dataStart = new LongWritable(dataStart);
            this.dataLength = new LongWritable(dataLength);

            this.idxPath = idxPath;
            this.idxTextPath = new Text(this.idxPath.toString());
            this.idxStart = new LongWritable(idxStart);
            this.idxLength = new LongWritable(idxLength);

            this.globalOffset = new LongWritable(globalOffset);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            this.dataTextPath.write(out);
            this.dataStart.write(out);
            this.dataLength.write(out);

            this.idxTextPath.write(out);
            this.idxStart.write(out);
            this.idxLength.write(out);

            this.globalOffset.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.dataTextPath.readFields(in);
            this.dataPath = new Path(this.dataTextPath.toString());
            this.dataStart.readFields(in);
            this.dataLength.readFields(in);

            this.idxTextPath.readFields(in);
            this.idxPath = new Path(this.idxTextPath.toString());
            this.idxStart.readFields(in);
            this.idxLength.readFields(in);

            this.globalOffset.readFields(in);
        }

        @Override
        public long getLength() {
            return dataLength.get() + idxLength.get();
        }

        @Override
        public String[] getLocations() {
            return new String[0];
        }

        public Path getDataPath() {
            return dataPath;
        }

        public long getDataStart() {
            return dataStart.get();
        }

        public long getDataLength() {
            return dataLength.get();
        }

        public Path getIdxPath() {
            return idxPath;
        }

        public long getIdxStart() { return idxStart.get(); }

        public long getIdxLength() {
            return idxLength.get();
        }

        public long getGlobalOffset() {
            return globalOffset.get();
        }
    }

    public static class PZKRecordReader extends RecordReader<LongWritable, Text> {
        FSDataInputStream dataInput;
        FSDataInputStream idxInput;

        long recordsNum;
        long curRecord = 0;
        int lastRecortLength = 0;

        LongWritable offset;
        Text document;

        byte[] intBuf = new byte[4];

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            PZKInputSplit fsplit = (PZKInputSplit) split;
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

            document = new Text("");
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if(curRecord >= recordsNum){
                return false;
            }
//            System.err.println("Record: " + Long.toString(curRecord) + "/" + Long.toString(recordsNum));
            offset.set(offset.get() + lastRecortLength);
            idxInput.readFully(intBuf);
            lastRecortLength = ByteBuffer.wrap(intBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
            try {
                document = new Text(decode(dataInput, lastRecortLength));
            } catch (DataFormatException exp){
                System.err.println("Invalid record at offset: " + offset.toString());
                return false;
            }
//            System.err.println("Finished: " + Long.toString(curRecord) + "/" + Long.toString(recordsNum));
            curRecord++;

            return true;
        }

        private String decode(FSDataInputStream dataFile, int bytesToRead) throws IOException, DataFormatException {
            Inflater decoder = new Inflater();
            byte[] encodedDataBuf = new byte[bytesToRead];

            int readedBytes, readedBytesSum = 0;
            while(readedBytesSum != bytesToRead) {
                readedBytes = dataFile.read(encodedDataBuf, readedBytesSum, bytesToRead - readedBytesSum);
                readedBytesSum += readedBytes;
            }
            decoder.setInput(encodedDataBuf);

            String result;
            int decodedDataBufSize = 1024;
            byte[] decodedDataBuf = new byte[decodedDataBufSize];
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {

                while (!decoder.finished()) {
                    int bytesDecoder = decoder.inflate(decodedDataBuf);
                    byteArrayOutputStream.write(decodedDataBuf, 0, bytesDecoder);
                }
                result = byteArrayOutputStream.toString("UTF-8");
            } finally {
                decoder.end();
            }
            return result;
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
            return (float) curRecord / recordsNum;
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
        return new PZKRecordReader();
    }

    private static long getMaxBytesPerSplit(Configuration conf) {
        return conf.getLong("mapreduce.input.indexedgz.bytespermap", 64 * 1024 * 1024);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>();
        FSDataInputStream idxInput;
        Configuration conf = context.getConfiguration();
        long maxSplitSize = getMaxBytesPerSplit(conf);
        long globalOffset = 0;

        for (FileStatus dataStatus: listStatus(context)) {
            Path dataPath = dataStatus.getPath();
            Path idxPath = dataPath.suffix(".idx");

            FileSystem fs = idxPath.getFileSystem(conf);
            idxInput = fs.open(idxPath);

            long dataLength = dataStatus.getLen();
            long procData = 0;
            int recordLength;
            long splitDataStart = 0;
            long splitDataLength = 0;
            long splitIdxStart = 0;
            long splitIdxLength = 0;
            byte[] intBuf = new byte[4];
            while (procData < dataLength) {
                idxInput.readFully(intBuf);
                recordLength = ByteBuffer.wrap(intBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
                if ((splitDataLength + recordLength < maxSplitSize) || (splitDataLength == 0)) {
                    splitDataLength += recordLength;
                    splitIdxLength += 4;
                } else {
                    splits.add(
                        new PZKInputSplit(
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
                    new PZKInputSplit(
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
