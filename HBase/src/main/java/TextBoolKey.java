import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextBoolKey implements WritableComparable<TextBoolKey> {
    private String host_;
    private boolean is_robots_;

    public static class Compator extends WritableComparator {
        public Compator() {
            super(TextBoolKey.class, true);
        }

        @Override
        public int compare(final WritableComparable key1, final WritableComparable key2) {
            return key1.compareTo(key2);
        }
    }

    public static class GroupComparator extends WritableComparator {
        public GroupComparator() {
            super(TextBoolKey.class, true);
        }

        @Override
        public int compare(final WritableComparable key1, final WritableComparable key2) {
            return ((TextBoolKey) key1).compareOnlyHost_((TextBoolKey) key2);
        }
    }

    public TextBoolKey() {
    }

    public TextBoolKey(final boolean is_robots, final String host) {
        is_robots_ = is_robots;
        host_ = host;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeBoolean(is_robots_);
        out.writeUTF(host_);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        is_robots_ = in.readBoolean();
        host_ = in.readUTF();
    }

    public int GetHash() {
        final int hash = host_.hashCode();
        return (hash > 0) ? hash : (-hash);
    }

    public String GetHost() {
        return host_;
    }

    public boolean IsRobots() {
        return is_robots_;
    }

    private int compareOnlyHost_(final TextBoolKey key2) {
        return host_.compareTo(key2.host_);
    }

    @Override
    public int compareTo(final TextBoolKey key2) {
        final int result_host_compare = compareOnlyHost_(key2);
        if (result_host_compare == 0) {
            if (is_robots_ == key2.is_robots_) { return 0; }
            if (is_robots_) { return -1; }
            else { return 1; }
        }
        return result_host_compare;
    }
}