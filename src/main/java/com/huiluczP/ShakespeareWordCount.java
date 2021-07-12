package com.huiluczP;

import com.huiluczP.count.WordCountMapper;
import com.huiluczP.count.WordCountReducer;
import com.huiluczP.sort.DecreasingCompare;
import com.huiluczP.sort.SortMapper;
import com.huiluczP.sort.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// main方法，设计job
public class ShakespeareWordCount {
    public static void main(String args[]){
        String dst = "hdfs://huiluczPc:8020/input/shakespeare_corpus";
        String dstOut = "hdfs://huiluczPc:8020/output/shakespeare_word_count_result";
        String sortedOut = "hdfs://huiluczPc:8020/output/shakespeare_word_count_sorted_result";

        Configuration hadoopConfig = new Configuration();
        try {
            Job job = Job.getInstance(hadoopConfig, ShakespeareWordCount.class.getSimpleName());

            job.setJarByClass(ShakespeareWordCount.class);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // 输出已存在则删除
            Path outPath = new Path(dstOut);
            outPath.getFileSystem(hadoopConfig).delete(outPath, true);
            //job执行作业时输入和输出文件的路径
            FileInputFormat.addInputPath(job, new Path(dst));
            FileOutputFormat.setOutputPath(job, new Path(dstOut));

            //执行job，直到完成
            job.waitForCompletion(true);
            System.out.println("Finished task1");

            // 排序
            Job jobSort =Job.getInstance(hadoopConfig, ShakespeareWordCount.class.getSimpleName() + " sort");
            jobSort.setJarByClass(ShakespeareWordCount.class);

            jobSort.setMapperClass(SortMapper.class);
            jobSort.setReducerClass(SortReducer.class);

            jobSort.setMapOutputKeyClass(IntWritable.class);
            jobSort.setMapOutputValueClass(Text.class);

            jobSort.setOutputKeyClass(Text.class);
            jobSort.setOutputValueClass(IntWritable.class);

            jobSort.setSortComparatorClass(DecreasingCompare.class);

            FileInputFormat.addInputPath(jobSort, new Path(dstOut));
            Path result=new Path(sortedOut);
            result.getFileSystem(hadoopConfig).delete(result, true);

            FileOutputFormat.setOutputPath(jobSort, result);

            jobSort.waitForCompletion(false);
            System.out.println("Finished task2");

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
