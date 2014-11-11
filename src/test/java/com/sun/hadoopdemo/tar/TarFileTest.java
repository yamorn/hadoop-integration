package com.sun.hadoopdemo.tar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class TarFileTest {
    private static TarFile.Reader reader;
    @BeforeClass
    public static void setUp() throws IOException{
        Configuration conf=new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.0.14:9000");
        conf.set("mapred.input.dir", "/usr/local/louis/tar/input/test.tar");
        conf.set("mapred.output.dir", "/usr/local/louis/tar/output");
        Path path=new Path("/usr/local/louis/tar/input/test.tar");
        FileSystem fs= FileSystem.get(conf);
        reader=new TarFile.Reader(fs,path,conf);
    }

    @Test
    @Ignore
    public void getCurrentKeyTest(){
        TarEntry entry= reader.getCurrentEntry();
        assertNotNull(entry);
    }
    @Test
    public void hasNextEntryTest() throws IOException {
        while (reader.hasNextEntry()) {
            System.out.println(reader.getCurrentEntry().getName());
        }
    }
    @AfterClass
    public static void tearDown() throws IOException{
        reader.close();
    }
}