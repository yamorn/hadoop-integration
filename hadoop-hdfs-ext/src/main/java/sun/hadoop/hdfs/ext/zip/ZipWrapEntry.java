package sun.hadoop.hdfs.ext.zip;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.zip.ZipEntry;

/**
 * Created by yamorn on 2014/12/3.
 */
public class ZipWrapEntry implements WritableComparable<ZipWrapEntry> {
    private ZipEntry zipEntry;

    public ZipWrapEntry(ZipEntry zipEntry) {
        this.zipEntry = zipEntry;
    }

    public ZipEntry getZipEntry() {
        return zipEntry;
    }

    public void setZipEntry(ZipEntry zipEntry) {
        this.zipEntry = zipEntry;
    }

    @Override
    public int compareTo(ZipWrapEntry o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, zipEntry.getName());
        out.writeLong(zipEntry.getTime());
        out.writeLong(zipEntry.getCrc());
        out.writeLong(zipEntry.getSize());
        out.writeLong(zipEntry.getCompressedSize());
        out.writeInt(zipEntry.getMethod());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String name = Text.readString(in);
        long time = in.readLong();
        long crc = in.readLong();
        long size = in.readLong();
        long csize = in.readLong();
        int method = in.readInt();

        ZipEntry entry = new ZipEntry(name);
        entry.setTime(time);
        entry.setCrc(crc);
        entry.setSize(size);
        entry.setCompressedSize(csize);
        entry.setMethod(method);
        this.zipEntry = entry;

    }
}
