package com.sun.hadoopdemo.zip;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.zip.ZipEntry;

/**
 * Created by louis on 2014/12/3.
 */
public class ZipFile {

    public static class Reader implements java.io.Closeable {
        private ZipWrapInputStream in;

        public Reader(FileSystem fs, Path file, Configuration conf)
                throws IOException {
            this.in = new ZipWrapInputStream(openFile(fs, file, conf.getInt("io.file.buffer.size", 4096)));
        }

        protected FSDataInputStream openFile(FileSystem fs, Path file, int bufferSize)
                throws IOException {
            return fs.open(file, bufferSize);
        }

        public synchronized boolean hasNextEntry() throws IOException {
            return in.hasNextEntry();
        }

        public synchronized ZipEntry getCurrentEntry() throws IOException {
            return in.getNextEntry();
        }

        public synchronized ZipContent getZipContent() {
            return in.getZipContent();
        }

        public synchronized ZipWrapEntry getZipWrapEntry() {
            return in.getZipWrapEntry();
        }


        public synchronized long getCurrentOffset() throws IOException {
            return in.getPos();

        }

        public synchronized void seek(long l) throws IOException {
            in.seek(l);
        }

        public synchronized void close() throws IOException {
            in.close();
        }

        public synchronized long skip(long l) throws IOException {
            return in.skip(l);
        }

    }
}
