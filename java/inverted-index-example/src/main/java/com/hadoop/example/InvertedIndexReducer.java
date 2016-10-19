package com.hadoop.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, TupleWritable, Text, Text> {
	protected void reduce(Text key, Iterable<TupleWritable> values, Context context)
			throws java.io.IOException, InterruptedException {
		StringBuilder documentAndIndexList = new StringBuilder();
		for (TupleWritable value : values) {
			if (documentAndIndexList.length() != 0) {
				documentAndIndexList.append(" ");
			}
			documentAndIndexList.append(value.get(0).toString() + " " + value.get(1).toString());
		}
		Text documentList = new Text();
		documentList.set(documentAndIndexList.toString());
		context.write(key, documentList);
	}
}
