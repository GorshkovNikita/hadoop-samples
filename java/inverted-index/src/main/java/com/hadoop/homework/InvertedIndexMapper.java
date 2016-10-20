package com.hadoop.homework;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Никита
 */
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Pair> {

	private Text word = new Text();

    protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		String[] line = value.toString().split("=");
        Integer index = 0;
		String documentName = line[0];
		String textStr = line[1];
		String[] wordArray = textStr.split(" ");
        for (String word : wordArray) {
            this.word.set(word);
            Pair docNameAndIndex = new Pair(documentName, index);
            context.write(this.word, docNameAndIndex);
            index += this.word.getLength() + 1;
        }
	}
}
