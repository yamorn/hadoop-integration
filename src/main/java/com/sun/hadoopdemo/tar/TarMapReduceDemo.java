package com.sun.hadoopdemo.tar;

import com.sun.hadoopdemo.tarmapreduce.TarFileInputFormat;
import com.sun.hadoopdemo.util.ImageUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by louis on 2014/11/11.
 */
public class TarMapReduceDemo {
    private final static String OUTPUT_DIR = "D:/test/";
    public static class TarMapper extends Mapper<TarHeader, TarEntry, TarHeader, TarEntry> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(TarHeader key, TarEntry value, Context context) throws IOException, InterruptedException {
            System.out.printf("File:%s\n",value.getName());
            context.write(key, value);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class TarReduce extends Reducer<TarHeader, TarEntry, Text, Text> {
        private static Text keyOut=new Text();
        private static Text valueOut=new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(TarHeader key, Iterable<TarEntry> values, Context context) throws IOException, InterruptedException {
            String fileName=key.name.toString();
            keyOut.set(fileName);
            int i=1;
            try {
                for (TarEntry entry : values) {
                    System.out.println(entry.getName()+"================");
                    System.out.println(entry.getContent().length+"+++++++++++");
                    byte[] content=entry.getContent();
//                    ImageUtils.pressText("水印 水印",content,OUTPUT_DIR+fileName+"_"+i+".jpg","宋体", Font.BOLD, Color.white, 80, 0, 0, 0.5f);
                    ImageUtils.gray(content, OUTPUT_DIR + fileName + "_" + i + ".jpg");
                    ++i;
                }
            } catch (Exception e) {
//                e.printStackTrace();
                valueOut.set("ERROR");
            }
            valueOut.set("OK");
            context.write(keyOut, valueOut);
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
        job.setReducerClass(TarReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TarFileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
