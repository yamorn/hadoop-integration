package com.sun.hadoopdemo.inputformat;

import com.sun.hadoopdemo.datatype.LogWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * Created by louis on 14-11-9.
 */
public class LogFileRecordReader extends RecordReader<LongWritable,LogWritable> {
    private LineRecordReader lineRecordReader;
    private LogWritable value;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader=new LineRecordReader();
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!lineRecordReader.nextKeyValue())
            return false;
        String line=lineRecordReader.getCurrentValue().toString();
        //Extract the fields from 'line'using a regex
        //value = new LogWritable(userIP, timestamp, request, status, bytes);
        value=new LogWritable();
        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return lineRecordReader.getCurrentKey();
    }

    @Override
    public LogWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
