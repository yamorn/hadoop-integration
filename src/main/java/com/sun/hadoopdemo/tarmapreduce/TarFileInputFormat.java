package com.sun.hadoopdemo.tarmapreduce;

import com.sun.hadoopdemo.tar.TarEntry;
import com.sun.hadoopdemo.tar.TarHeader;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
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
//        List<InputSplit> splits = new ArrayList<InputSplit>();
//        List<FileStatus>files = listStatus(job);
//        for (FileStatus file: files) {
//            Path path = file.getPath();
//            FileSystem fs = path.getFileSystem(job.getConfiguration());
//            long length = file.getLen();
//            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
//            if ((length != 0) && isSplitable(job, path)) {
//                long blockSize = file.getBlockSize();
//                long bytesRemaining = length;
//
//            }
//        }
//
//        return null;
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        return super.listStatus(job);
    }
}
