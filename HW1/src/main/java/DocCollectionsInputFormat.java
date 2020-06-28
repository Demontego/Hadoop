import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import com.google.common.io.LittleEndianDataInputStream;

public class DocCollectionsInputFormat extends FileInputFormat<LongWritable, Text> {
    public class DocRecordReader extends RecordReader<LongWritable, Text> {

        FSDataInputStream input_file;
        FSDataInputStream input_index;
        Text text;
        List<Integer> index;
        int max_size_buf;
        byte[] buf;
        byte[] uncompressed_buf;
        int num_doc;
        long all_doc;
        long first_file;

        @Override
        public void initialize(final InputSplit split,final TaskAttemptContext context)
                throws IOException, InterruptedException {
            final Configuration conf = context.getConfiguration();
            final FileSplit fsplit = (FileSplit) split;
            final Path path = fsplit.getPath();
            final String index_string = fsplit.getPath() + ".idx";
            final Path index_path = new Path(path.getParent(), index_string);
            final FileSystem fs = path.getFileSystem(conf);
            input_index = fs.open(index_path);
            prepare_index(fsplit, path, fs, input_index);
        }

        private void prepare_index(final FileSplit fsplit, final Path path, final FileSystem fs,
                final FSDataInputStream input_index) throws IOException {
            index = read_index(input_index);
            first_file = fsplit.getStart();
            long offset = 0;
            while (num_doc < first_file) {
                offset += index.get(num_doc);
                ++num_doc;
            }
            all_doc = fsplit.getLength();
            input_file = fs.open(path);
            input_file.seek(offset);
            max_size_buf=Collections.max(index);
            uncompressed_buf = new byte[max_size_buf*10];//maybe 
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (num_doc >= all_doc)
                return false;
            buf = new byte[index.get(num_doc)];
            input_file.readFully(buf, 0, index.get(num_doc));
            final Inflater decompressor = new Inflater();
            decompressor.setInput(buf);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while (!decompressor.needsInput() & !decompressor.finished()){
                int len = 0;
                try {
                    len = decompressor.inflate(uncompressed_buf);
                } catch (final DataFormatException e) {
                    e.printStackTrace();
                }
                baos.write(uncompressed_buf, 0, len);
                //true_len = baos.size();

            }
            decompressor.end();
            baos.close();
            text = new Text(baos.toString(StandardCharsets.UTF_8.name()));
            num_doc++;
            return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable((index.get(num_doc - 1)));
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return text;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float) (num_doc) / all_doc;
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeStream(input_file);
        }
    }

    public RecordReader<LongWritable, Text> createRecordReader(final InputSplit split, final TaskAttemptContext context)
            throws IOException, InterruptedException {
        final DocRecordReader reader = new DocRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    private static List<Integer> read_index(final FSDataInputStream index_file) throws IOException {
        final LittleEndianDataInputStream in = new LittleEndianDataInputStream(index_file);
        final List<Integer> tmp = new ArrayList<>();
        try {
            while (true) {
                tmp.add(in.readInt());
            }
        } catch (final EOFException ignored) {
        }
        return tmp;
    }

    public List<InputSplit> getSplits(final JobContext context) throws IOException {
        final List<InputSplit> splits = new ArrayList<>();

        for (final FileStatus status : listStatus(context)) {
            final Configuration conf = context.getConfiguration();
            final long bytes_for_split = getNumBytesPerSplit(conf);
            final Path path = status.getPath();
            String index_string = path.getName();
            if (index_string.substring(index_string.length() - 4).equals(".idx")) {
                continue;
            } else {
                index_string = index_string + ".idx";
            }
            final FileSystem fs = path.getFileSystem(conf);
            final Path index_path = new Path(path.getParent(), index_string);
            final FSDataInputStream input_index = fs.open(index_path);
            final List<Integer> indx = read_index(input_index);
            int cur_split = 0;
            long split_size = 0;
            long offset = 0;
            for (final Integer cur : indx) {
                split_size += cur;
                cur_split++;
                if (split_size > bytes_for_split) {
                    splits.add(new FileSplit(path, offset, cur_split, null));
                    offset += cur_split;
                    split_size = 0;
                    cur_split = 0;
                }
            }
            splits.add(new FileSplit(path, offset, cur_split, null));
        }
        return splits;
    }

    public static final String BYTES_PER_MAP = "mapreduce.input.doc.bytes_per_map";

    public static long getNumBytesPerSplit(final Configuration conf) {
        return  conf.getLong(BYTES_PER_MAP, 134217728);
    }
}
