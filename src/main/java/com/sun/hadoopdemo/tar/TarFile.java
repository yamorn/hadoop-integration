package com.sun.hadoopdemo.tar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by louis on 14-11-11.
 */
public class TarFile {

    public static class Reader implements java.io.Closeable{
        private Path file;
        private Configuration conf;
        private TarInputStream in;

        public Reader(FileSystem fs, Path file, Configuration conf)
                throws IOException {
            this.file = file;
            this.in = new TarInputStream(openFile(fs, file, conf.getInt("io.file.buffer.size", 4096)));
            this.conf = conf;
        }

        protected FSDataInputStream openFile(FileSystem fs, Path file, int bufferSize)
                throws IOException {
            return fs.open(file, bufferSize);
        }

        public synchronized TarEntry getNextEntry() throws IOException {
            return in.getNextEntry();
        }

        public synchronized boolean hasNextEntry() throws IOException{
            return in.hasNextTarEntry();
        }

        public synchronized long getCurrentOffset() {
            return in.getCurrentOffset();

        }

        public TarEntry getCurrentEntry(){
            return in.getCurrentEntry();
        }

        public synchronized void close() throws IOException {
            in.close();
        }
    }
}