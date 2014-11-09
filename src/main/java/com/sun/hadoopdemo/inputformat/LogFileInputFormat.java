package com.sun.hadoopdemo.inputformat;

import com.sun.hadoopdemo.datatype.LogWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by louis on 14-11-9.
 */
public class LogFileInputFormat extends FileInputFormat<LongWritable,LogWritable> {
    @Override
    public RecordReader<LongWritable, LogWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new LogFileRecordReader();
    }
}
