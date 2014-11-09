package com.sun.hadoopdemo.datatype;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by louis on 14-11-9.
 */

/**
 * Apache http log
 * example:
 * 192.168.0.2 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/  HTTP/1.0" 200 6245
 */
public class LogWritable implements WritableComparable {
    private Text ip;
    private Text timestamp;
    private Text request;
    private IntWritable responseSize;
    private IntWritable status;
    public LogWritable(){
        this.ip=new Text();
        this.timestamp=new Text();
        this.request=new Text();
        this.responseSize=new IntWritable();
        this.status=new IntWritable();
    }

    public LogWritable(Text ip, Text timestamp, Text request, IntWritable responseSize, IntWritable status) {
        this.ip=ip;
        this.timestamp=timestamp;
        this.request=request;
        this.responseSize=responseSize;
        this.status=status;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ip.write(out);
        timestamp.write(out);
        request.write(out);
        responseSize.write(out);
        status.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ip.readFields(in);
        timestamp.readFields(in);
        request.readFields(in);
        responseSize.readFields(in);
        status.readFields(in);
    }

    public Text getIp() {
        return ip;
    }

    public void setIp(Text ip) {
        this.ip = ip;
    }

    public Text getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Text timestamp) {
        this.timestamp = timestamp;
    }

    public Text getRequest() {
        return request;
    }

    public void setRequest(Text request) {
        this.request = request;
    }

    public IntWritable getResponseSize() {
        return responseSize;
    }

    public void setResponseSize(IntWritable responseSize) {
        this.responseSize = responseSize;
    }

    public IntWritable getStatus() {
        return status;
    }

    public void setStatus(IntWritable status) {
        this.status = status;
    }
}
