package com.hadoop.homework;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Никита
 */
public class InvertedIndexReducer extends Reducer<Text, Pair, Text, PairList> {
	protected void reduce(Text key, Iterable<Pair> values, Context context)
			throws java.io.IOException, InterruptedException {

        PairList documentAndIndexList = new PairList(Pair.class);
        List<Pair> arr = new ArrayList<Pair>();
        for (Pair pair : values)
            arr.add(new Pair(pair.getDocName(), pair.getIndex()));

        documentAndIndexList.set(arr.toArray(new Pair[arr.size()]));
        context.write(key, documentAndIndexList);
	}
}
