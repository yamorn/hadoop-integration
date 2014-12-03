package com.sun.hadoopdemo.zip;

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
public class ZipFileInputFormatTest {

    private static ZipFileInputFormat zipFileInputFormat;
    private static JobContext jobContext;
    @BeforeClass
    public static void init() {
        Configuration conf=new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.0.14:9000");
        conf.set("mapred.input.dir", "/user/hadoop/pic.zip");
        conf.set("mapred.output.dir", "/usr/hadoop/output");
        jobContext = new JobContext(conf, new JobID());
        zipFileInputFormat = new ZipFileInputFormat();
    }

    @Test
    @Ignore
    public void listStatusTest() throws IOException {
        List<FileStatus> list=zipFileInputFormat.listStatus(jobContext);
        assertTrue(list.size()>0);
        for (FileStatus fs : list) {
            System.out.println(fs.getPath());
        }

    }
    @Test
    public void getSplitsTest() throws Exception {
        List<InputSplit> list=zipFileInputFormat.getSplits(jobContext);
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