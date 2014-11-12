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
        private TarInputStream in;

        public Reader(FileSystem fs, Path file, Configuration conf)
                throws IOException {
            this.in = new TarInputStream(openFile(fs, file, conf.getInt("io.file.buffer.size", 4096)));
        }

        protected FSDataInputStream openFile(FileSystem fs, Path file, int bufferSize)
                throws IOException {
            return fs.open(file, bufferSize);
        }

        public synchronized boolean hasNextEntry() throws IOException{
            return in.hasNextTarEntry();
        }

        public synchronized long getCurrentOffset() throws IOException{
            return in.getPos();

        }

        public synchronized TarEntry getCurrentEntry(){
            return in.getCurrentEntry();
        }

        public synchronized void seek(long l) throws IOException{
            in.seek(l);
        }

        public synchronized void close() throws IOException {
            in.close();
        }
        public synchronized long skip(long l) throws IOException{
            return in.skip(l);
        }

        public synchronized long indexTarEntryHeader(long offset,long end) throws IOException {
            return in.indexTarEntryHeader(offset, end);
        }
    }
}
