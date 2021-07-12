package com.huiluczP.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// 利用counter对词汇数量进行计数
// 对values数量进行判断，小于5次的计数
// filename_开头的key另外处理，计数
// 正常输出count结果
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 对key判断，文件名或正常词汇
        if(key.toString().startsWith("filename_")){
            System.out.println(key.toString());
            // 文件名，直接计数
            Counter c = context.getCounter("mycounters", "file_num_counter");
            c.increment(1);
        }else{
            // 正常处理
            int valueNum = 0;
            for(IntWritable v:values){
                valueNum += v.get();
            }
            if(valueNum < 5){
                // 出现次数小于5，计数
                Counter c = context.getCounter("mycounters", "less_5_counter");
                c.increment(1);
            }
            // 不同词汇计数
            Counter c = context.getCounter("mycounters", "voc_num_counter");
            c.increment(1);
            context.write(new Text(key.toString()), new IntWritable(valueNum));
        }
    }
}
