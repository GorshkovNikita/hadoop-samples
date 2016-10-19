package com.hadoop.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.AbstractMap;
import java.util.Map;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, TupleWritable> {

	private Text wordText = new Text();

    protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		String[] line = value.toString().split("=");
        Integer index = 0;
		String documentName = line[0];
		String textStr = line[1];
		String[] wordArray = textStr.split(" ");
		for (int i = 0; i < wordArray.length; i++) {
			wordText.set(wordArray[i]);
            TupleWritable docNameAndIndex = new TupleWritable(new Writable[] {new Text(documentName), new IntWritable(index)});
//            docNameAndIndex.set("( " + documentName + ", " + Integer.toString(index) + " )");
			context.write(wordText, docNameAndIndex);
            index += wordText.getLength() + 1;
		}
	}
}
