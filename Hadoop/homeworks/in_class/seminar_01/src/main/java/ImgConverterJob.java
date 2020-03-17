import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;

public class ImgConverterJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public class ImgConverterMapper extends Mapper<NullWritable, BytesWritable, Text, NullWritable> {
        @Override
        protected void map(NullWritable key, BytesWritable image, Context context)
                throws IOException, InterruptedException
        {
            byte[] jpg = ImageConverter.Convert(image.getBytes(), "jpg");
            Text jpg_base64 = new Text(Base64.encodeBase64(jpg));
            context.write(jpg_base64, NullWritable.get());
        }
    }

    public static Job GetJobConf(Configuration conf, String input, String out_dir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(ImgConverterJob.class);
        job.setJobName(ImgConverterJob.class.getCanonicalName());

        job.setInputFormatClass(BMPCollectionInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        job.setMapperClass(ImgConverterMapper.class);
        job.setNumReduceTasks(0); // this will be map-only job

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ImgConverterJob(), args);
        System.exit(exitCode);
    }
}
