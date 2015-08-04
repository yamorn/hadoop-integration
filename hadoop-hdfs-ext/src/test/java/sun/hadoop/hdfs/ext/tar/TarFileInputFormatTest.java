package sun.hadoop.hdfs.ext.tar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by yamorn on 14-11-10.
 */

@RunWith(JUnit4.class)
public class TarFileInputFormatTest {
    private static TarFileInputFormat tarFileInputFormat;
    private static JobContext jobContext;
    @BeforeClass
    public static void init() throws IOException {
        Configuration conf=new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.0.14:9000");
        conf.set("mapred.input.dir", "/usr/local/louis/tar/input/test.tar");
        conf.set("mapred.output.dir", "/usr/local/louis/tar/output");
//        JobClient jobClient=new JobClient(conf);
//        JobStatus[] jobStatus=jobClient.getAllJobs();
//        for (JobStatus jobStatus1 : jobStatus) {
//            RunningJob runningJob=jobClient.getJob(jobStatus1.getJobID());
//            if (runningJob.getJobName().equals("")) {
//            }
//        }

//        jobContext = new JobContext(conf, new JobID());
//        tarFileInputFormat = new TarFileInputFormat();
    }

    @Test
    @Ignore
    public void listStatusTest() throws IOException {
        List<FileStatus> list=tarFileInputFormat.listStatus(jobContext);
        assertTrue(list.size()>0);
        for (FileStatus fs : list) {
            System.out.println(fs.getPath());
        }

    }
    @Test
    @Ignore
    public void getSplitsTest() throws Exception {
        List<InputSplit> list=tarFileInputFormat.getSplits(jobContext);
        assertTrue(list.size()>0);
        for (InputSplit split : list) {
            String[] locs=split.getLocations();
            String temp="";
            for (String s : locs) {
                temp+=s+" ";
            }
            System.out.println("locs:"+temp+" length:"+split.getLength());
        }
    }
//    @AfterClass
//    public static void clean() {
//
//    }

}