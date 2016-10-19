package com.hadoop.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Никита
 */
public class InvertedIndexReducer extends Reducer<Text, Pair, Text, Text> {
	protected void reduce(Text key, Iterable<Pair> values, Context context)
			throws java.io.IOException, InterruptedException {
		StringBuilder documentAndIndexList = new StringBuilder();
		for (Pair value : values) {
			if (documentAndIndexList.length() != 0) {
				documentAndIndexList.append(" ");
			}
			documentAndIndexList.append(value.toString());
		}
		Text documentList = new Text();
		documentList.set(documentAndIndexList.toString());
		context.write(key, documentList);
	}
}
