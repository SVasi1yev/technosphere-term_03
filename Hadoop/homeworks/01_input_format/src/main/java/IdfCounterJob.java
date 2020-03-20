import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class IdfCounterJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        return ;
    }

    static public class IdfCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String document = value.toString().toLowerCase();
            Pattern pattern = Pattern.compile("[A-Za-zА-Яа-я0-9]+");
            Matcher matcher = pattern.matcher(document);
            while (matcher.find()) {
                document.substring(matcher.start(), matcher.end());
            }
        }
    }


}
