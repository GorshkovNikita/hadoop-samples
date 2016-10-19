package com.hadoop.example;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Никита
 */
public class InvertedIndexReducer extends Reducer<Text, Pair, Text, MyArrayWritable> {
	protected void reduce(Text key, Iterable<Pair> values, Context context)
			throws java.io.IOException, InterruptedException {
        
        MyArrayWritable documentAndIndexList = new MyArrayWritable(Pair.class);
        Pair[] arr = new Pair[1000];
        int i = 0;
        for (Iterator<Pair> iter = values.iterator(); iter.hasNext(); ) {
            Pair docNameAndIndex = iter.next();
            arr[i] = docNameAndIndex;
            i++;
        }
        documentAndIndexList.set(arr);
		context.write(key, documentAndIndexList);
	}
}
