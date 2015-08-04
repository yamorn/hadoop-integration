package sun.hadoop.hdfs.ext.tar;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by yamorn on 14-11-10.
 */

@RunWith(JUnit4.class)
public class TarFileTest {
    private static TarFile.Reader reader;
//    @BeforeClass
//    public static void setUp() throws IOException{
//        Configuration conf=new Configuration();
//        conf.set("fs.default.name", "hdfs://localhost:9000");
//        conf.set("mapred.input.dir", "/user/louis/input/test.tar");
//        conf.set("mapred.output.dir", "/user/louis/output");
//        Path path=new Path("/user/louis/input/test.tar");
//        FileSystem fs= FileSystem.get(conf);
//        reader=new TarFile.Reader(fs,path,conf);
//    }

    @Test
    @Ignore
    public void getCurrentKeyTest(){
        TarEntry entry= reader.getCurrentEntry();
        assertNotNull(entry);
    }
    @Test
    @Ignore
    public void hasNextEntryTest() throws IOException {
        int i=1;
        while (reader.hasNextEntry()) {
            System.out.println(reader.getCurrentEntry().getName());
            byte[] content=reader.getCurrentEntry().getContent();
            System.out.println(content.length);
//            FileOutputStream fileOutputStream = new FileOutputStream("D:/test/tt_" + (i++) + ".jpg");
//            fileOutputStream.write(content,0,content.length);
//            fileOutputStream.close();

//            System.out.println(new String(content,0,content.length));
        }
    }
//    @AfterClass
//    public static void tearDown() throws IOException{
//        reader.close();
//    }
}