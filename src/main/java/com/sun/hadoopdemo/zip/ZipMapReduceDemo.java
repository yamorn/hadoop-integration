package com.sun.hadoopdemo.zip;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by louis on 2014/12/3.
 */
public class ZipMapReduceDemo {
    public static class ZipMapper extends Mapper<ZipWrapEntry, ZipContent, Text, Text> {
        private static Path rootPath;
        private static FileSystem fileSystem;
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            fileSystem = FileSystem.get(conf);
            rootPath = new Path(conf.get("convert.output"));
        }
        @Override
        protected void map(ZipWrapEntry key, ZipContent value, Context context) throws IOException, InterruptedException {
            String fileName = key.getZipEntry().getName();
            long length = value.getContent().length;
            OutputStream outputStream = fileSystem.create(new Path(rootPath, fileName));
            IOUtils.write(value.getContent(), outputStream);
            IOUtils.closeQuietly(outputStream);
            context.write(new Text(fileName), new Text(length + ""));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.set("fs.default.name", "hdfs://192.168.0.14:9000");
        conf.set("mapred.job.tracker", "192.168.0.14:9001");
        conf.set("convert.output","/user/hadoop/fileoutput");
        String otherArgs[] = new String[]{
                "/user/hadoop/pic.zip",
                "/user/hadoop/output"
        };
        Job job = new Job(conf, "ZipMapReduceDemo");
        job.setJarByClass(ZipMapReduceDemo.class);
        job.setMapperClass(ZipMapper.class);

        job.setMapOutputKeyClass(TextOutputFormat.class);
        job.setMapOutputValueClass(TextOutputFormat.class);

        job.setOutputKeyClass(TextOutputFormat.class);
        job.setOutputValueClass(TextOutputFormat.class);

        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        ZipFileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
