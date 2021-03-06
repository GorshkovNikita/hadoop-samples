package com.hadoop.homework;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * @author Никита
 */
public class InvertedIndexJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: InvertedIndexJob <input path> <output path>");
            System.exit(-1);
        }
    
        Job job = new Job();
        job.setJarByClass(com.hadoop.homework.InvertedIndexJob.class);
        job.setJobName("Inverted Index");
   
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        job.setMapperClass(com.hadoop.homework.InvertedIndexMapper.class);
        job.setReducerClass(com.hadoop.homework.InvertedIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Pair.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArrayWritable.class);

        job.waitForCompletion(true);
    }
}
