package sun.hadoop.hdfs.ext.other;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by yamorn on 14-11-9.
 */
public class DfsUtils {
    private Logger logger = Logger.getLogger(getClass().getName());
    private Configuration conf;

    public DfsUtils(Configuration conf) {
        this.conf = conf;
        if (conf.get("fs.default.name").equals("file:///")) {
            conf.set("fs.default.name", "hdfs://localhost:9000");
        }
    }

    public void traverse(String file, PathFilter pathFilter, DfsFileHandler handler) throws Exception {
        FileSystem fs = FileSystem.get(URI.create("/"), conf);
        Path path = new Path(file);
        _traverse(fs, path, pathFilter, handler);
        fs.close();
    }

    private void _traverse(FileSystem fs, Path path, PathFilter pathFilter, DfsFileHandler handler) throws IOException {
        if (fs.isFile(path)) {
            handler.handler(path);
        } else {
            FileStatus[] list = fs.listStatus(path, pathFilter);
            for (FileStatus stat : list) {
                _traverse(fs, stat.getPath(), pathFilter, handler);
            }
        }
    }

    public void mkdirs(String dir) throws Exception {
        FileSystem fs = FileSystem.get(URI.create("/"), conf);
        Path path = new Path(dir);
        if (!fs.exists(path)) {
            if (!fs.mkdirs(path)) {
                logger.warning("Create dir " + path.toString() + " failed.");
            }
        } else {
            logger.info(dir + " already exists.");
        }
        fs.close();
    }

    public void rm(String file, boolean recursive) throws Exception {
        FileSystem fs = FileSystem.get(URI.create("/"), conf);
        Path path = new Path(file);
        if (!fs.delete(path, recursive)) {
            logger.warning("Delete path " + file + " failed.");
        }
        fs.close();
    }

    public void createFile(String file, String content, boolean overwrite) throws Exception {
        FileSystem fs = FileSystem.get(URI.create("/"), conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file), overwrite);
            os.write(buff, 0, buff.length);
        } finally {
            if (os != null) {
                os.close();
            }
        }
        fs.close();
    }

    public String cat(String file) throws Exception {
        FileSystem fs = FileSystem.get(URI.create("/"), conf);
        Path path = new Path(file);
        if (!fs.exists(path)) {
            logger.warning(file + " doesn't exist.");
            return null;
        }
        if (!fs.isFile(path)) {
            logger.warning(file + " isn't a file.");
            return null;
        }
        FSDataInputStream is = null;
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            is = fs.open(path);
            IOUtils.copyBytes(is, os, 4096);
        } finally {
            IOUtils.closeStream(is);
            IOUtils.closeStream(os);
        }
        fs.close();
        return os.toString();
    }

    public void copyFromLocal(String src, String dst, boolean delSrc, boolean overwrite) throws Exception {
        FileSystem fs = FileSystem.get(URI.create("/"), conf);
        File file = new File(src);
        if (!file.exists()) {
            logger.log(Level.SEVERE, src + " doesn't exists.");
            return;
        }
        Path dstPath = new Path(dst);
        fs.copyFromLocalFile(delSrc, overwrite, new Path(src), dstPath);
        fs.close();
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("fs.default.name","hdfs://localhost:9000");
        DfsUtils dfsUtils = new DfsUtils(conf);

////        dfsUtils.copyFromLocal("/home/louis/dic.txt","/home/dic.txt",false,false);
//        String str=dfsUtils.cat("/home/dic.txt");
//        System.out.println(str);

    }
}
