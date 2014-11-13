package com.sun.hadoopdemo.tarmapreduce;

import com.sun.hadoopdemo.tar.TarEntry;
import com.sun.hadoopdemo.tar.TarHeader;
import com.sun.hadoopdemo.tar.TarInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by louis on 14-11-11.
 */
public class TarFileInputFormat extends FileInputFormat<TarHeader, TarEntry> {
    static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

    @Override
    public RecordReader<TarHeader, TarEntry> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new TarFileRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        Configuration conf = job.getConfiguration();
        List<InputSplit> splits = new ArrayList<>();
        List<FileStatus> files = listStatus(job);
        for (FileStatus file : files) {
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(conf);
            long length = file.getLen();
            FSDataInputStream fsDataInputStream = fs.open(path);
            TarInputStream tarInputStream = new TarInputStream(fsDataInputStream);
            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
            if ((length != 0) && isSplitable(job, path)) {
                long blockSize = file.getBlockSize();
                long bytesRead = 0;
                for (int i = 0; i < blkLocations.length; i++) {
                    ArrayList<String> hosts = new ArrayList<>();
                    int startIndex = 0, endIndex = 0;
                    if (i == blkLocations.length - 1) {
                        startIndex = getBlockIndex(blkLocations, bytesRead);
                        endIndex = getBlockIndex(blkLocations, file.getLen() - 1);
                        for (int j = startIndex; j <= endIndex; j++) {
                            String[] blkHosts = blkLocations[j].getHosts();
                            for (int k = 0; k < blkHosts.length; k++)
                                hosts.add(blkHosts[k]);
                        }
                        splits.add(new FileSplit(path, bytesRead, file.getLen() - bytesRead, hosts.toArray(new String[hosts.size()])));
                        continue;
                    }
                    long p=blockSize*(i+1);
                    tarInputStream.seek(p);
                    long headerPos = tarInputStream.indexTarEntryHeader(p, blockSize+p);
                    if(headerPos==-1){
                        continue;
                    }
                    startIndex = getBlockIndex(blkLocations, bytesRead);
                    endIndex = getBlockIndex(blkLocations, headerPos);

                    for (int j = startIndex; j <= endIndex; j++) {
                        String[] blkHosts = blkLocations[j].getHosts();
                        for (int k = 0; k < blkHosts.length; k++)
                            hosts.add(blkHosts[k]);
                    }

                    splits.add(new FileSplit(path, bytesRead, headerPos - bytesRead, hosts.toArray(new String[hosts.size()])));

                    bytesRead = headerPos;


                }
            }
            fsDataInputStream.close();
            tarInputStream.close();
            fs.close();
        }
        job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
        return splits;
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        return super.listStatus(job);
    }
}
