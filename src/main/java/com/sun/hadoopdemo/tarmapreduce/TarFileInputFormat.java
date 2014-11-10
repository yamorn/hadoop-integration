package com.sun.hadoopdemo.tarmapreduce;

import com.sun.hadoopdemo.tar.TarEntry;
import com.sun.hadoopdemo.tar.TarHeader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * Created by louis on 14-11-11.
 */
public class TarFileInputFormat extends FileInputFormat<TarHeader,TarEntry> {
    @Override
    public RecordReader<TarHeader, TarEntry> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new TarFileRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        return super.getSplits(job);
    }
}
