package com.sun.hadoopdemo.tar;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by louis on 14-11-13.
 */
public class Test {
    public static void main(String[] args) {
        Path src = Paths.get("/home/louis/test/pic/zzz.jpg");
        for(int i=1;i<=40;i++) {
            Path dst = Paths.get("/home/louis/test/pic/zzz_" + i + ".jpg");
            try {
                Files.copy(src,dst);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
