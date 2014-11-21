package com.sun.hadoopdemo.tar;

import com.sun.hadoopdemo.tarmapreduce.TarFileInputFormat;
import com.sun.hadoopdemo.tarmapreduce.TarOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
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

    public static class TarReduce extends Reducer<TarHeader, TarEntry, Text, TarEntry> {
        private static Text keyOut = new Text("test"); //path prefix

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(TarHeader key, Iterable<TarEntry> values, Context context) throws IOException, InterruptedException {
            TarEntry valueOut=null;
            try {
                for (TarEntry entry : values) {
                    valueOut=entry;
                    System.out.println("filename:"+entry.getName()+" size:"+(entry!=null?entry.getSize():"null")+" content:"+entry.getContent().length);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            context.write(keyOut, valueOut);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.set("fs.default.name", "hdfs://192.168.1.10:9000");
        String otherArgs[] = new String[]{
                "/user/hadoop/file.tar",
                "/user/hadoop/output"
        };
        Job job = new Job(conf, "TarMapReduceDemo");
        job.setJarByClass(TarMapReduceDemo.class);
        job.setMapperClass(TarMapper.class);
        job.setReducerClass(TarReduce.class);

        job.setMapOutputKeyClass(TarHeader.class);
        job.setMapOutputValueClass(TarEntry.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TarEntry.class);

        job.setInputFormatClass(TarFileInputFormat.class);
        job.setOutputFormatClass(TarOutputFormat.class);

        TarFileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TarOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
