package sun.hadoop.hdfs.ext.zip;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by yamorn on 2014/12/3.
 */
public class ZipFileRecordReader extends RecordReader<ZipWrapEntry, ZipContent> {
    private static Logger logger = Logger.getLogger(ZipFileRecordReader.class.getName());
    private ZipFile.Reader reader;
    private long start;
    private long end;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        Path path = fileSplit.getPath();
        Configuration conf = context.getConfiguration();
        FileSystem fs = path.getFileSystem(conf);
        start = fileSplit.getStart();
        reader = new ZipFile.Reader(fs, path, conf);
        end = fileSplit.getStart() + fileSplit.getLength();
        if (start != 0) {
            reader.seek(start);
        }
        logger.log(Level.INFO, "Split initialize: start at " + start + " , end at " + end);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        System.out.println("offset="+reader.getCurrentOffset()+", end="+end);
        return (reader.getCurrentOffset() < end && reader.hasNextEntry());
    }

    @Override
    public ZipWrapEntry getCurrentKey() throws IOException, InterruptedException {
        return reader.getZipWrapEntry();
    }

    @Override
    public ZipContent getCurrentValue() throws IOException, InterruptedException {
        return reader.getZipContent();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (end == start) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (float) ((reader.getCurrentOffset() - start) / (double) (end - start)));
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
