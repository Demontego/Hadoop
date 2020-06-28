import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.net.MalformedURLException;
import java.net.URL;


public class FilterHbase extends Configured implements Tool {

    private static final String WebsitesTableNameParameter = "websites_name";
    private static final String WebpagesTableNameParameter = "webpages_name";
    private static final String DisabledLabel = "#D#";
    private static final String EnabledLabel = "#E#";

    // Table -> <host, U+url> (из таблицы webpages) или <host, R+robots> (из таблицы
    // websites)
    public static class FilterHbaseMapper extends TableMapper<TextBoolKey, Text> {
        @Override
        protected void map(final ImmutableBytesWritable rowKey, final Result columns, final Context context)
                throws IOException, InterruptedException {
            final TableSplit current_split = (TableSplit) context.getInputSplit();
            final String table_name = new String(current_split.getTableName());

            final String websites_table_name = context.getConfiguration().get(WebsitesTableNameParameter);
            final String webpages_table_name = context.getConfiguration().get(WebpagesTableNameParameter);

            if (table_name.equals(websites_table_name)) {
                final String site_name = new String(columns.getValue(Bytes.toBytes("info"), Bytes.toBytes("site")));

                if (columns.containsColumn(Bytes.toBytes("info"), Bytes.toBytes("robots"))) {
                    String robots = new String(columns.getValue(Bytes.toBytes("info"), Bytes.toBytes("robots")));
                    context.write(new TextBoolKey(true, site_name), new Text(robots));
                }
            }

            else if (table_name.equals(webpages_table_name)) {
                String url_string = new String(columns.getValue(Bytes.toBytes("docs"), Bytes.toBytes("url")));
                final URL url = new URL(url_string);
                final boolean is_enabled = (columns.getValue(Bytes.toBytes("docs"), Bytes.toBytes("disabled")) == null);
                if (is_enabled) {
                    url_string = EnabledLabel + url_string;
                } else {
                    url_string = DisabledLabel + url_string;
                }
                context.write(new TextBoolKey(false, url.getHost()), new Text(url_string));
            }

            else {
                throw new InterruptedException(
                        "Unknown table: " + table_name + "; " + webpages_table_name + "; " + websites_table_name);
            }
        }
    }

    public static class RobotsFilterPartitioner extends Partitioner<TextBoolKey, Text> {
        @Override
        public int getPartition(final TextBoolKey key, final Text val, final int num_partitions) {
            return key.GetHash() % num_partitions;
        }
    }

    // <Text, [Text]> -> <url, Y/N>
    public static class FilterHbaseReducer extends TableReducer<TextBoolKey, Text, ImmutableBytesWritable> {

        private static byte[] GetMD5Hash(final String for_hash) throws InterruptedException {
            final byte[] bytesOfMessage = for_hash.getBytes();

            MessageDigest md = null;
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (final NoSuchAlgorithmException exc) {
                throw new InterruptedException(exc.getMessage());
            }
            final byte[] digest = md.digest(bytesOfMessage);
            return DatatypeConverter.printHexBinary(digest).toLowerCase().getBytes();
        }

        private static boolean IsDisabled(final Robots_txt filter, final String url_string)
                throws InterruptedException, MalformedURLException {
            try{
                final URL extractor = new URL(url_string);
            
            final String ref = extractor.getRef();
            String for_check;
            if (ref != null) {
                for_check = extractor.getFile() + "#" + extractor.getRef();
            } else {
                for_check = extractor.getFile();
            }
            if (for_check == null) {
                return false;
            }

            boolean disabled = false;
            if (filter != null) {
                disabled = !filter.IsAllowed(for_check);
            }

            return disabled;
            }catch(Exception e){
                return false;
            }
        }

        @Override
        protected void reduce(final TextBoolKey website, final Iterable<Text> vals, final Context context)
                throws IOException, InterruptedException {
            Robots_txt filter = null;
            int iteration = 0;

            for (final Text value : vals) {
                String current_str = value.toString();

                if (website.IsRobots()) {
                    if (iteration != 0) {
                        throw new InterruptedException("robots.txt should be first in the list!");
                    }
                    if (filter != null) {
                        throw new InterruptedException("Too much robots.txt for one site!");
                    }
                    filter = new Robots_txt(current_str);

                }
                iteration++;

                final boolean is_disabled_on_start = current_str.startsWith(DisabledLabel);
                current_str = current_str.substring(DisabledLabel.length());
                final byte[] hash = GetMD5Hash(current_str);

                final boolean disabled = IsDisabled(filter, current_str);
                if (disabled && !is_disabled_on_start) {
                    final Put put = new Put(hash);
                    put.add(Bytes.toBytes("docs"), Bytes.toBytes("disabled"), Bytes.toBytes("Y"));
                    context.write(null, put);
                } else if (!disabled && is_disabled_on_start) {
                    final Delete delete = new Delete(hash);
                    delete.deleteColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
                    context.write(null, delete);
                }
            }
        }
    }

    private List<Scan> GetScans(final String[] input_names) {
        final List<Scan> scans = new ArrayList<>();

        for (final String name : input_names) {
            final Scan scan = new Scan();
            scan.setAttribute("scan.attributes.table.name", Bytes.toBytes(name));
            scans.add(scan);
        }
        return scans;
    }


    private Job getJobConf(final String webpages_name, final String websites_name) throws IOException {
        final Job job = Job.getInstance(getConf());
        job.setJarByClass(FilterHbase.class);
        job.setJobName(FilterHbase.class.getCanonicalName());

        final Configuration conf = job.getConfiguration();
        conf.set(WebpagesTableNameParameter, webpages_name);
        conf.set(WebsitesTableNameParameter, websites_name);

        job.setNumReduceTasks(2);

        final String[] names = { webpages_name, websites_name };
        final List<Scan> scans = GetScans(names);
        TableMapReduceUtil.initTableMapperJob(scans, FilterHbaseMapper.class, TextBoolKey.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(webpages_name, FilterHbaseReducer.class, job);

        job.setGroupingComparatorClass(TextBoolKey.GroupComparator.class);
        job.setSortComparatorClass(TextBoolKey.Compator.class);
        job.setPartitionerClass(RobotsFilterPartitioner.class);

        return job;
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Job job = getJobConf(args[0], args[1]);
        // final FileSystem fs = FileSystem.get(getConf());
        // if (fs.exists(new Path(args[1]))) {
        //     fs.delete(new Path(args[1]), true);
        // }
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(final String[] args) throws Exception {
        final int ret = ToolRunner.run(new FilterHbase(), args);
        System.exit(ret);
    }
}