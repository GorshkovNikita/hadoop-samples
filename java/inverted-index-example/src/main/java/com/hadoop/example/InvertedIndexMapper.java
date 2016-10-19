package com.hadoop.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.AbstractMap;
import java.util.Map;

/**
 * @author Никита
 */
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Pair> {

	private Text wordText = new Text();

    protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		String[] line = value.toString().split("=");
        Integer index = 0;
		String documentName = line[0];
		String textStr = line[1];
		String[] wordArray = textStr.split(" ");
        for (String word : wordArray) {
            wordText.set(word);
            Pair docNameAndIndex = new Pair(documentName, index);
            context.write(wordText, docNameAndIndex);
            System.out.println(wordText + " " + docNameAndIndex.toString());
            index += wordText.getLength() + 1;
        }
	}
}
