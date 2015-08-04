package sun.hadoop.hdfs.ext.tar;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by yamorn on 14-11-10.
 */
@RunWith(JUnit4.class)
public class TarInputStreamTest {
    private static TarInputStream tarInputStream;
    //    @BeforeClass
//    public static void init() throws IOException{
//        Configuration conf=new Configuration();
//        conf.set("fs.default.name", "hdfs://localhost:9000");
//        Path path=new Path("/user/louis/input/pic.tar");
//        FileSystem fs=FileSystem.get(URI.create("/"),conf);
//        tarInputStream = new TarInputStream(fs.open(path));
////        FSDataInputStream
//    }
    @Test
    @Ignore
    public void getCurrentEntryTest() throws IOException {
        TarEntry entry=null;
//        while ((entry = tarInputStream.getNextEntry()) != null) {
//            System.out.println(entry.getName());
//            System.out.println(tarInputStream.getCurrentOffset());
//        }
//        System.out.println(tarInputStream.getCurrentOffset());
        int i=1;
        while (tarInputStream.hasNextTarEntry()) {
            entry=tarInputStream.getCurrentEntry();
            FileOutputStream out = new FileOutputStream(new File("/home/louis/test/picc/zzz_" + (i++) + ".jpg"));
            out.write(entry.getContent());
            out.close();
            System.out.println(entry.getName()+" length:"+tarInputStream.getCurrentEntry().getSize()+" offset="+ tarInputStream.getPos());
        }
    }
    @Test
    @Ignore
    public void currentPosTest() throws IOException{

//        System.out.println(tarInputStream.getPos());
//        System.out.println(tarInputStream.getCurrentOffset());

//        System.out.println("pos="+tarInputStream.getCurrentOffset()+" curEntryName="+tarInputStream.getCurrentEntry().getName());
//        System.out.println(tarInputStream.hasNextTarEntry());
//        System.out.println("pos="+tarInputStream.getCurrentOffset()+" curEntryName="+tarInputStream.getCurrentEntry().getName());

    }
    @Test
    @Ignore
    public void indexTarEntryHeaderTest() throws IOException{
//        tarInputStream.getNextEntry();
//        tarInputStream.getNextEntry();
        long offset=tarInputStream.getPos();
        System.out.println("offset="+offset);
        long index=tarInputStream.indexTarEntryHeader(offset, 1000000);
        System.out.println(index+"====");
    }
//    @AfterClass
//    public static void clean() throws IOException{
//        if(tarInputStream!=null)
//            tarInputStream.close();
//    }
}