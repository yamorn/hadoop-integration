package sun.hadoop.hdfs.ext.zip;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yamorn on 2014/12/3.
 */
public class ZipContent implements WritableComparable<ZipContent> {
    private long length;
    private byte[] content;
    public ZipContent(){}

    public ZipContent(byte[] content) {
        this.content=content;
        length=content.length;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
        this.length=content.length;
    }

    @Override
    public int compareTo(ZipContent o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(length);
        out.write(content);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        long length=in.readLong();
        byte[] temp = new byte[(int)length];
        in.readFully(temp);
        this.content = temp;
    }
}
