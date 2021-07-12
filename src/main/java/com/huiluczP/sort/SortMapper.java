package com.huiluczP.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// 将key和value倒转，方便排序
public class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] keyValueStrings = line.split("\t");
        int outKey = Integer.parseInt(keyValueStrings[1]);
        String outValue = keyValueStrings[0];
        context.write(new IntWritable(outKey), new Text(outValue));
    }
}
