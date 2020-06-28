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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class SecondarySortDemo extends Configured implements Tool {
    public static void main(final String[] args) throws Exception {
        final int rc = ToolRunner.run(new SecondarySortDemo(), args);
        System.exit(rc);
    }

    @Override
    public int run(final String[] args) throws Exception {
        final FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        final Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class FindReducer extends Partitioner<TextTextPair, IntWritable> {
        @Override
        public int getPartition(final TextTextPair key, final IntWritable val, final int numPartitions) {
            return Math.abs(key.getFirst().hashCode()) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(TextTextPair.class, true);
        }

        @Override
        public int compare(final WritableComparable a, final WritableComparable b) {
            return ((TextTextPair) a).compareTo((TextTextPair) b);
        }
    }

    public static class Grouper extends WritableComparator {
        protected Grouper() {
            super(TextTextPair.class, true);
        }

        @Override
        public int compare(final WritableComparable a, final WritableComparable b) {
            final Text a_first = ((TextTextPair) a).getFirst();
            final Text b_first = ((TextTextPair) b).getFirst();
            return a_first.compareTo(b_first);
        }
    }

    public static class TermalMapper extends Mapper<LongWritable, Text, TextTextPair, IntWritable> {
        URI uri;
        static final IntWritable one = new IntWritable(1);
        @Override
        protected void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            final String[] lines = value.toString().split("\t");
            String host;
            try {
                uri = new URI(lines[1]);
                host = uri.getHost();
                host = host.startsWith("www.") ? host.substring(4) : host;
            } catch (final URISyntaxException | NullPointerException e) {
                context.getCounter("COMMON_COUNTERS", "BadURLS").increment(1);
                return;
            }
            final TextTextPair composie = new TextTextPair(host, lines[0]);
            context.write(composie, one);
        }
    }

    public static class TermalReducer extends Reducer<TextTextPair, IntWritable, Text, IntWritable> {
        static final String MIN_CLICKS = "mapreduce.reduce.seo.minclicks";
        @Override
        protected void reduce(final TextTextPair key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {
            final long min_clicks = 10;// context.getConfiguration().getLong(MIN_CLICKS, 1);
            values.iterator().next().get();
            int sum = 1;
            int most_important_sum = 1;
            String most_important_query = key.getSecond().toString();
            String cur_query = key.getSecond().toString();
            for (final IntWritable ignored : values) {
                if (!cur_query.equals(key.getSecond().toString())) {
                    if (sum > most_important_sum) {
                        most_important_sum = sum;
                        most_important_query = cur_query;
                    }
                    cur_query = key.getSecond().toString();
                    sum = 0;
                }
                ++sum;
            }
            if (most_important_sum >= min_clicks)
                context.write(new Text(key.getFirst() + "\t" + most_important_query),
                        new IntWritable(most_important_sum));
        }
    }

    Job getJobConf(final String input, final String out_dir) throws IOException {
        final Job job = Job.getInstance(getConf());
        job.setJarByClass(SecondarySortDemo.class);
        job.setJobName(SecondarySortDemo.class.getCanonicalName());
        FileInputFormat.setInputDirRecursive(job, true);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        job.setMapperClass(TermalMapper.class);
        job.setReducerClass(TermalReducer.class);

        job.setPartitionerClass(FindReducer.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(Grouper.class);

        job.setMapOutputKeyClass(TextTextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}
