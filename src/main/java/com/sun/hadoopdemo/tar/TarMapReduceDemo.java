package com.sun.hadoopdemo.tar;

import com.sun.hadoopdemo.tarmapreduce.TarFileInputFormat;
import com.sun.hadoopdemo.tarmapreduce.TarOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by louis on 2014/11/11.
 */
public class TarMapReduceDemo {
    private final static String OUTPUT_DIR = "D:/test/ee/";

    public static class TarMapper extends Mapper<TarHeader, TarEntry, TarHeader, TarEntry> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(TarHeader key, TarEntry value, Context context) throws IOException, InterruptedException {
            System.out.printf("Map File:%s\n", value.getName());
            context.write(key, value);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class TarReduce extends Reducer<TarHeader, TarEntry, TarHeader, TarEntry> {
        private static Text keyOut = new Text();
//        private static Text valueOut = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(TarHeader key, Iterable<TarEntry> values, Context context) throws IOException, InterruptedException {
            String fileName = key.name.toString();
            keyOut.set(fileName);
            int i = 1;
            TarEntry valueOut=null;
            try {
                for (TarEntry entry : values) {
                    valueOut=entry;
                    System.out.println("filename:"+entry.getName()+" size:"+(entry!=null?entry.getSize():"null")+" content:"+entry.getContent().length);
//                    byte[] content = entry.getContent();
//                    try (ByteArrayInputStream in = new ByteArrayInputStream(content);
//                         FileOutputStream out = new FileOutputStream(new File(OUTPUT_DIR + entry.getName()))) {
//                        IOUtils.copyBytes(in, out, 2048, false);
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                    ImageUtils.gray(content, OUTPUT_DIR + fileName + "_" + i + ".jpg");
                    ++i;
                }
            } catch (Exception e) {
//                e.printStackTrace();
//                valueOut.set("ERROR");
            }
//            valueOut.set("OK");
//            valueOut.set(values[0]);
            context.write(key, valueOut);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String otherArgs[] = new String[]{
                "hdfs://localhost:9000/user/louis/input/test.tar",
                "hdfs://localhost:9000/user/louis/output"
        };
        Job job = new Job(conf, "TarMapReduceDemo");
        job.setJarByClass(TarMapReduceDemo.class);

        job.setMapperClass(TarMapper.class);
        job.setReducerClass(TarReduce.class);

        job.setMapOutputKeyClass(TarHeader.class);
        job.setMapOutputValueClass(TarEntry.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TarFileInputFormat.class);
        job.setOutputFormatClass(TarOutputFormat.class);

        TarFileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TarOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
