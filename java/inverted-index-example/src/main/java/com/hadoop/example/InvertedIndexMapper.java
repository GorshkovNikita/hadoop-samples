package com.hadoop.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Sample Data Document=text
 * T[0]=it is what it is
 * T[1]=what is it
 * T[2]=it is a banana
 */

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text wordText = new Text();
	private final static Text docNameAndIndex = new Text();

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		String[] line = value.toString().split("=");

		String documentName = line[0];
		String textStr = line[1];
		String[] wordArray = textStr.split(" ");
		for(int i = 0; i < wordArray.length; i++) {
			wordText.set(wordArray[i]);
            docNameAndIndex.set("( " + documentName + ", " + Integer.toBinaryString(i) + " )");
			context.write(wordText, docNameAndIndex);
		}
	}
}
