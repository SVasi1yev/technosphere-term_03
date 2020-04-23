import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


// Обрабатываем статистику GSOD используя сортировку средствами mapreduce
// Задача является демонстрационной и не нацелена быть оптимальной
public class SecondarySortDemo extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new SecondarySortDemo(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class MDStationPartitioner extends Partitioner<TextFloatPair, IntWritable> {
        @Override
        public int getPartition(TextFloatPair key, IntWritable val, int numPartitions) {
            // Для каждой метеостанции в конкретный день месяца - отправляем на один reducer
            return Math.abs(key.getFirst().hashCode()) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(TextFloatPair.class, true /* десериализовывать ли объекты (TextFloatPair) для compare */);
        }

        /*
         * Сортируем по полному ключу:
         * - id метеостанции
         * - день и месяц
         * - если совпали - по убыванию температуры (см TextFloatPair.compareTo)
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((TextFloatPair)a).compareTo((TextFloatPair)b);
        }
    }

    public static class MDStationGrouper extends WritableComparator {
        protected MDStationGrouper() {
            super(TextFloatPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            // считаем за группу текстовую часть ключа: {id станции, день, месяц}
            Text a_first = ((TextFloatPair)a).getFirst();
            Text b_first = ((TextFloatPair)b).getFirst();
            return a_first.compareTo(b_first);
        }
    }

    public static class TermalMapper extends Mapper<LongWritable, Text, TextFloatPair, IntWritable>
    {
        GSODRecord gsod = new GSODRecord();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            gsod.parseFrom(line);

            if (gsod.hasMaxTemp()) {
                /*
                 * Формируем композитный ключ специальной формы: <{id станции, день, месяц}, макс_температура_в_этот_день
                 * В качестве значения используем год.
                 */
                String natural_key = String.format("%d:%02d.%02d", gsod.station, gsod.day, gsod.month);
                TextFloatPair composite = new TextFloatPair(natural_key, gsod.max_temp);
                context.write(composite, new IntWritable(gsod.year));
            }
            else {
                context.getCounter("COMMON_COUNTERS", "SkippedValues").increment(1);
            }
        }
    }


    public static class TermalReducer extends Reducer<TextFloatPair, IntWritable, Text, Text> {
        @Override
        /*
         * Обратите внимание: мы не проходимся по values, а просто берем первый элемент.
         * Mapreduce с помощью сортировки по композитному ключу уже все сделал за нас.
         * А значит, в первом значении будет самая высокая температура этой метеостанции
         *   за конкретный день конкретного месяца. выводим ее вместе с годом.
         */
        protected void reduce(TextFloatPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int year = values.iterator().next().get();
            float temp = key.getSecond().get();

            String value = String.format("%.1fC (%d)", temp, year);
            context.write(key.getFirst(), new Text(value));
        }
    }

    Job GetJobConf(Configuration conf, String input, String out_dir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(SecondarySortDemo.class);
        job.setJobName(SecondarySortDemo.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        job.setMapperClass(TermalMapper.class);
        job.setReducerClass(TermalReducer.class);

        job.setPartitionerClass(MDStationPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(MDStationGrouper.class);

        // выход mapper-а != вывод reducer-а, поэтому ставим отдельно
        job.setMapOutputKeyClass(TextFloatPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}
