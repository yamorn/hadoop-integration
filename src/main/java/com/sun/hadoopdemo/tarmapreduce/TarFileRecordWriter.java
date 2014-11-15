package com.sun.hadoopdemo.tarmapreduce;

import com.sun.hadoopdemo.tar.TarEntry;
import com.sun.hadoopdemo.tar.TarFile;
import com.sun.hadoopdemo.tar.TarHeader;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by louis on 2014/11/14.
 */
public class TarFileRecordWriter extends RecordWriter<TarHeader,TarEntry> {
    private TarFile.Writer out;
    public TarFileRecordWriter(TarFile.Writer out){
        this.out=out;
    }
    @Override
    public synchronized void write(TarHeader key, TarEntry value) throws IOException, InterruptedException {
//        Path path=new Path("/user/louis/output/"+ FilenameUtils.getBaseName(key.name.toString()));
        Path path = new Path(FilenameUtils.getBaseName(key.name.toString()));
        out.writeEntry(path, value);
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException, InterruptedException {
        out.close();
    }

}
