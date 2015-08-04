package sun.hadoop.hdfs.ext.zip;

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
import java.util.LinkedList;
import java.util.List;

/**
 * Created by yamorn on 2014/12/3.
 */
public class ZipFileInputFormat extends FileInputFormat<ZipWrapEntry,ZipContent> {
    static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
    @Override
    public RecordReader<ZipWrapEntry, ZipContent> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ZipFileRecordReader();
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        return super.listStatus(job);
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
            ZipWrapInputStream inputStream = new ZipWrapInputStream(fsDataInputStream);
            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
            LinkedList<Long> offsets = new LinkedList<>();
            long socdStart=inputStream.getSOCDStartIndex(length);
            if(socdStart==-1){
                throw new IOException("Can't find central directory start.");
            }
            inputStream.seek(socdStart);
            CDHeader cdHeader=null;
            while ((cdHeader = inputStream.getNextCDFH()) != null) {
                offsets.add(cdHeader.getLochOffset());
            }

            if ((length != 0) && isSplitable(job, path)) {
                long blockSize = file.getBlockSize();
                long bytesRead = 0;
                int index=0;
                for (int i = 0; i < blkLocations.length; i++) {
                    ArrayList<String> hosts = new ArrayList<>();
                    int startIndex = 0, endIndex = 0;

                    long p=blockSize*(i+1);
                    long headerPos=-1;
                    while (index < offsets.size()) {
                        long temp = offsets.get(index);
                        if (temp >= p) {
                            headerPos = temp;
                            break;
                        }
                        index++;
                    }
//                    for (int n = 0; n < offsets.size(); n++) {
//                        long temp = offsets.get(n);
//                        if (temp >= p) {
//                            headerPos=temp;
//                            break;
//                        }
//                    }
                    if(headerPos==-1){
                        headerPos=file.getLen()-1;
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
            inputStream.close();
            fs.close();
        }
        job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

        return splits;
    }
}
