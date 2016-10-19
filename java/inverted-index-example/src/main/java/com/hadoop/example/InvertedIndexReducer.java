package com.hadoop.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;

/**
 * @author Никита
 */
public class InvertedIndexReducer extends Reducer<Text, Pair, Text, MyArrayWritable> {
	protected void reduce(Text key, Iterable<Pair> values, Context context)
			throws java.io.IOException, InterruptedException {

        MyArrayWritable documentAndIndexList = new MyArrayWritable(Pair.class);
        ArrayList<Pair> arr = new ArrayList<Pair>();
        for (Pair docNameAndIndex : values) {
            arr.add(docNameAndIndex);
        }
        documentAndIndexList.set(arr.toArray(new Pair[arr.size()]));
		context.write(key, documentAndIndexList);
	}
}
