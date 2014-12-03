package com.sun.hadoopdemo.zip;

/**
 * Created by louis on 2014/12/3.
 */
public class CDHeader {
    private String fileName;
    private long lochOffset;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getLochOffset() {
        return lochOffset;
    }

    public void setLochOffset(long lochOffset) {
        this.lochOffset = lochOffset;
    }
}
