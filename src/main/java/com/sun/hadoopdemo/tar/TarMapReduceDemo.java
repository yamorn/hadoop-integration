package com.sun.hadoopdemo.tar;

import com.sun.hadoopdemo.tarmapreduce.TarFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by louis on 2014/11/11.
 */
public class TarMapReduceDemo {
    public static class TarMapper extends Mapper<TarHeader, TarEntry, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(TarHeader key, TarEntry value, Context context) throws IOException, InterruptedException {
            System.out.println(value.getName());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        String otherArgs[] = new String[]{
                "hdfs://192.168.0.14:9000/usr/local/louis/tar/input/test.tar",
                "hdfs://192.168.0.14:9000/usr/local/louis/tar/output"
        };
        Job job = new Job(conf, "wordCount");
        job.setJarByClass(TarMapReduceDemo.class);

        job.setMapperClass(TarMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TarFileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
