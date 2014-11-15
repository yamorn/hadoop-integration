package com.sun.hadoopdemo.tarmapreduce;


import com.sun.hadoopdemo.tar.TarEntry;
import com.sun.hadoopdemo.tar.TarFile;
import com.sun.hadoopdemo.tar.TarHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by louis on 2014/11/14.
 */
public class TarOutputFormat extends FileOutputFormat<TarHeader,TarEntry> {
    @Override
    public RecordWriter<TarHeader, TarEntry> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        // get the path of the temporary output file
        Path file = getDefaultWorkFile(job, "");
        FileSystem fs = file.getFileSystem(conf);
        final TarFile.Writer out =new TarFile.Writer(fs);
        return new TarFileRecordWriter(out);
    }

}
