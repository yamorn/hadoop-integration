package com.sun.hadoopdemo.tarmapreduce;

import com.sun.hadoopdemo.tar.TarEntry;
import com.sun.hadoopdemo.tar.TarFile;
import com.sun.hadoopdemo.tar.TarHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by louis on 14-11-10.
 */
public class TarFileRecordReader extends RecordReader<TarHeader, TarEntry> {
    private TarFile.Reader in;
    private long start;
    private long end;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit=(FileSplit)split;
        Path path = fileSplit.getPath();
        Configuration conf = context.getConfiguration();
        FileSystem fs = path.getFileSystem(conf);
        start=fileSplit.getStart();
        in = new TarFile.Reader(fs, path, conf);
        end = fileSplit.getStart() + split.getLength();
        if(start!=0){
            in.seek(start);
            long index=in.indexTarEntryHeader(start,end);
            System.out.println("index="+index+ " start="+start);
            in.skip(index - start);
            System.out.println("skip:"+(index - start));
            System.out.println("offset="+in.getCurrentOffset()+" "+in.getCurrentEntry());
            start=index;
        }
        System.out.println("Block init:split start "+start+" ,end at    "+end);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return in.hasNextEntry();
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
