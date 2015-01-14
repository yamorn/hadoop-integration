package com.sun.hadoopdemo.example;

import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExampleMapperTest {
    @Test
    @Ignore
    public void ignoresMissingTemperatureRecord() {
        ExampleMapper mapper=new ExampleMapper();
        String str = "0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9+99991+99999999999";
        Text text=new Text(str);
//        ExampleMapper.Context context = mock(ExampleMapper.Context.class);
    }
}