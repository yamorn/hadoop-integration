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
        Path src = Paths.get("D:\\test\\ee\\201405285A_742224E.jpg");
        for(int i=1;i<=250;i++) {
            Path dst = Paths.get("D:\\test\\ee\\zzz_" + i + ".jpg");
            try {
                Files.copy(src,dst);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
