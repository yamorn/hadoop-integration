package sun.hadoop.hdfs.ext.zip;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.zip.ZipEntry;

import static org.junit.Assert.*;

/**
 *  Created by yamorn on 2014/12/3.
 */

@RunWith(JUnit4.class)
public class ZipFileTest {
    private static ZipFile.Reader reader;

//    @BeforeClass
//    public static void setUp() throws IOException {
//        Configuration conf = new Configuration();
//        conf.set("fs.default.name", "hdfs://192.168.0.14:9000");
//        conf.set("mapred.output.dir", "/usr/hadoop/output");
//        Path path = new Path("/user/hadoop/pic.zip");
//        FileSystem fs = FileSystem.get(conf);
//        reader = new ZipFile.Reader(fs, path, conf);
//    }

    @Test
    @Ignore
    public void getCurrentKeyTest() throws IOException {
        ZipEntry entry = reader.getCurrentEntry();
        assertNotNull(entry);
    }

    @Test
    @Ignore
    public void hasNextEntryTest() throws IOException {
        int i = 1;
        while (reader.hasNextEntry()) {
            String fileName = reader.getCurrentEntry().getName();
            System.out.println(fileName);
//            byte[] content=reader.getZipContent().getContent();
//            System.out.println(content.length);
//            FileOutputStream fileOutputStream = new FileOutputStream("D:\\test\\ee\\"+fileName);
//            fileOutputStream.write(content,0,content.length);
//            fileOutputStream.close();

//            System.out.println(new String(content,0,content.length));
        }
    }

//    @AfterClass
//    public static void tearDown() throws IOException {
//        reader.close();
//    }
}