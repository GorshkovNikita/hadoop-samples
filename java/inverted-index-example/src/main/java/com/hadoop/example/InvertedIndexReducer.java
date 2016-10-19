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
public class InvertedIndexReducer extends Reducer<Text, Pair, Text, ArrayWritable> {
	protected void reduce(Text key, Iterable<Pair> values, Context context)
			throws java.io.IOException, InterruptedException {
//		StringBuilder documentAndIndexList = new StringBuilder();
        ArrayWritable documentAndIndexList = new ArrayWritable(Pair.class);
        List<Pair> array = new ArrayList<Pair>();
        int i = 0;
        for (Iterator<Pair> iter = values.iterator(); iter.hasNext(); ) {
            Pair docNameAndIndex = iter.next();
            array.add(docNameAndIndex);
            i++;
        }
        documentAndIndexList.set((Writable[]) array.toArray());
		Text documentList = new Text();
//		documentList.set(documentAndIndexList.toString());
		context.write(key, documentAndIndexList);
	}
}
