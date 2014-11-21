package com.sun.hadoopdemo.tarmapreduce;

import com.sun.hadoopdemo.tar.TarEntry;
import com.sun.hadoopdemo.tar.TarFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.File;
import java.io.IOException;

/**
 * Created by louis on 2014/11/14.
 */
public class TarFileRecordWriter extends RecordWriter<Text, TarEntry> {
    private TarFile.Writer out;

    public TarFileRecordWriter(TarFile.Writer out) {
        this.out = out;
    }

    @Override
    public synchronized void write(Text key, TarEntry value) throws IOException, InterruptedException {
        String pathPrefix = key.toString();
        String fileName = value.getName();
        if (pathPrefix.charAt(pathPrefix.length() - 1) == File.separatorChar) {
            pathPrefix = pathPrefix.substring(0, pathPrefix.length() - 1);
        }
        Path path = new Path(pathPrefix + File.separator + fileName);
        out.writeEntry(path, value);
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException, InterruptedException {
        out.close();
    }

}
