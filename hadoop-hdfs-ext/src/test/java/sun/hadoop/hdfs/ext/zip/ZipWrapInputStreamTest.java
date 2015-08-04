package sun.hadoop.hdfs.ext.zip;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.*;

/**
 * Created by yamorn on 2014/12/3.
 */

@RunWith(JUnit4.class)
public class ZipWrapInputStreamTest {
    private static ZipWrapInputStream inputStream;
    private static long fileLength;
//    @BeforeClass
//    public static void setUp() throws Exception {
//        Configuration conf=new Configuration();
//        conf.set("fs.default.name", "hdfs://192.168.0.14:9000");
//        Path path=new Path("/user/hadoop/test.zip");
//        FileSystem fs=FileSystem.get(URI.create("/"),conf);
//        fileLength=fs.getFileStatus(path).getLen();
//        inputStream = new ZipWrapInputStream(fs.open(path));
//    }

//    @AfterClass
//    public static void tearDown() throws Exception {
//        inputStream.close();
//    }

    @org.junit.Test
    @Ignore
    public void testGetEOCDStartIndex() throws Exception {
        long index = inputStream.getSOCDStartIndex(fileLength);
        System.out.println(index);
        inputStream.seek(index);
        CDHeader cdHeader=null;
        while ((cdHeader = inputStream.getNextCDFH()) != null) {
            System.out.printf("name=%s,offset=%d\n", cdHeader.getFileName(), cdHeader.getLochOffset());
        }
//        inputStream.seek(28920);
//        ZipEntry zipEntry=null;
//        while ((zipEntry = inputStream.getNextEntry()) != null) {
//            System.out.println(zipEntry.getName());
//            FileOutputStream outputStream = new FileOutputStream("D:\\test\\ee\\" + zipEntry.getName());
//
//            IOUtils.copy(inputStream.getZipInputStream(), outputStream);
//            outputStream.close();
//        }
    }
}