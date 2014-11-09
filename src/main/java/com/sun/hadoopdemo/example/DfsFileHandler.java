package com.sun.hadoopdemo.example;

import org.apache.hadoop.fs.Path;

/**
 * Created by louis on 14-11-9.
 */
public interface DfsFileHandler {
    public void handler(Path path);

}
