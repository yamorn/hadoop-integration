package sun.hadoop.hdfs.ext.tar;

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
 * Created by yamorn on 14-11-10.
 */
public class TarFileRecordReader extends RecordReader<TarHeader, TarEntry> {
    private static Logger logger = Logger.getLogger(TarFileRecordReader.class.getName());
    private TarFile.Reader in;
    private long start;
    private long end;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        Path path = fileSplit.getPath();
        Configuration conf = context.getConfiguration();
        FileSystem fs = path.getFileSystem(conf);
        start = fileSplit.getStart();
        in = new TarFile.Reader(fs, path, conf);
        end = fileSplit.getStart() + fileSplit.getLength();
        if (start != 0) {
            in.seek(start);
//            long index = in.indexTarEntryHeader(start, end);
//            in.skip(index - start);
//            start = index;
        }
        logger.log(Level.INFO, "Split initialize: start at " + start + " , end at " + end);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        long content = 0;
        if (in.getCurrentEntry() != null) {
            content = in.getCurrentEntry().getHeader().size;
            content += TarConstants.HEADER_BLOCK - content % TarConstants.HEADER_BLOCK;
        }
        return ((in.getCurrentOffset() + content) < end) && in.hasNextEntry();
    }

    @Override
    public TarHeader getCurrentKey() throws IOException, InterruptedException {
        return in.getCurrentEntry().getHeader();
    }

    @Override
    public TarEntry getCurrentValue() throws IOException, InterruptedException {
        return in.getCurrentEntry();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (end == start) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (float) ((in.getCurrentOffset() - start) / (double) (end - start)));
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
