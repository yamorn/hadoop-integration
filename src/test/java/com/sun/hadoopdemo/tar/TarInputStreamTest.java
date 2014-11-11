package com.sun.hadoopdemo.tar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;
@RunWith(JUnit4.class)
public class TarInputStreamTest {
    private static TarInputStream tarInputStream;
    @BeforeClass
    public static void init() throws IOException{
        Configuration conf=new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.0.14:9000");
        Path path=new Path("/usr/local/louis/tar/input/test.tar");
        FileSystem fs=FileSystem.get(URI.create("/"),conf);
        tarInputStream = new TarInputStream(fs.open(path));
    }
    @Test
    public void getCurrentEntryTest() throws IOException {
        TarEntry entry=null;
//        while ((entry = tarInputStream.getNextEntry()) != null) {
//            System.out.println(entry.getName());
//            System.out.println(tarInputStream.getCurrentOffset());
//        }
        System.out.println(tarInputStream.getCurrentOffset());
        while (tarInputStream.hasNextTarEntry()) {
            entry=tarInputStream.getNextEntry();
            System.out.println(entry.getName());
        }
    }
    @AfterClass
    public static void clean() throws IOException{
        tarInputStream.close();
    }
}