package com.sun.hadoopdemo.tarmapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class TarFileInputFormatTest {
    private static TarFileInputFormat tarFileInputFormat;
    private static JobContext jobContext;
    @BeforeClass
    public static void init() {
        Configuration conf=new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.0.14:9000");
        conf.set("mapred.input.dir", "/usr/local/louis/tar/input/test.tar");
        conf.set("mapred.output.dir", "/usr/local/louis/tar/output");
        jobContext = new JobContext(conf, new JobID());
        tarFileInputFormat = new TarFileInputFormat();
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
    @AfterClass
    public static void clean() {

    }

}