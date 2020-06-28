import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.*;
public class InitPageRankJob extends Configured implements Tool {

    public static class InitPageRankMapper extends Mapper<Text, Text, Text, Text> {
        private final Text ZeroText = new Text("");
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (!key.toString().startsWith("#")) {
                context.write(key, value);
                context.write(value, ZeroText);
            }
        }
    }
    public static class InitPageRankReducer extends Reducer<Text, Text, Text, Text> {
        private final Text ZeroText = new Text("");
        Double mass = 10000.0/4847571;
        private MultipleOutputs<Text, Text> out;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
        }
        @Override
        protected void reduce(Text key, Iterable<Text> text, Context context) throws IOException, InterruptedException {
            StringBuilder linksOut = new StringBuilder();
            linksOut.append(mass);
            boolean notLeak = false;
            for (Text i:text){
                if (!i.equals(ZeroText)) {
                    if (!notLeak){
                        linksOut.append("\t");
                        notLeak = true;
                    }
                    linksOut.append(i.toString()).append(",");
                }
            }
            if (notLeak) {
                linksOut.delete(linksOut.length() - 1, linksOut.length());
            }
            context.write(key, new Text(linksOut.toString()));
        }
        @Override
        protected void cleanup(Context context
        ) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(InitPageRankJob.class);
        job.setJobName(InitPageRankJob.class.getCanonicalName());

        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setMapperClass(InitPageRankMapper.class);
        job.setReducerClass(InitPageRankReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        final FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        //BasicConfigurator.configure();
        int ret = ToolRunner.run(new InitPageRankJob(), args);
        System.exit(ret);
    }
}

