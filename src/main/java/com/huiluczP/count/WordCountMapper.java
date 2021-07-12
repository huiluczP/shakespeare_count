package com.huiluczP.count;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

// 分词操作
// 利用counter计算不同词汇数量
// 将文件名也作为map传入
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private ArrayList<String> stopWords = null;

    // 读取一个stop_word集合
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 读取stop_word文件，存入内存
        Path path = new Path("hdfs://huiluczPc:8020/input/stop_word.txt");
        Configuration conf = new Configuration();
        FileSystem fileSystem = path.getFileSystem(conf);

        FSDataInputStream fsdis = fileSystem.open(path);
        LineReader lineReader = new LineReader(fsdis, conf);

        stopWords = new ArrayList<String>();
        Text line = new Text();
        while(lineReader.readLine(line) > 0){
            stopWords.add(line.toString());
        }
        lineReader.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 分词操作
        StringTokenizer itr = new StringTokenizer(value.toString(), " \r\t\n!,.:?-(){}[]<>/\\+@*~#%&;\"\'");
        while(itr.hasMoreTokens()){
            String token = itr.nextToken().toLowerCase(); // 改为小写，防止重复
            // 对非停止词处理
            if(!stopWords.contains(token)) {
                if (token.startsWith("t")) {
                    // t或T开头出现则+1
                    Counter c = context.getCounter("mycounters", "t_prefix_counter");
                    c.increment(1);
                }
                // 总词数
                Counter c1 = context.getCounter("mycounters", "word_num_counter");
                c1.increment(1);

                context.write(new Text(token), new IntWritable(1));
            }
        }
        // 切割路径，获取文件名，以filename_为前缀传入key
        String path = ((FileSplit)context.getInputSplit()).getPath().toString();
        int index = path.lastIndexOf("/");
        String fileName = path.substring(index+1);
        context.write(new Text("filename_" + fileName), new IntWritable(1));
    }
}
