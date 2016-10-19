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
            arr.add(new Pair(docNameAndIndex.getDocName(), docNameAndIndex.getIndex()));
            System.out.println(docNameAndIndex.toString());
            //            str += docNameAndIndex.toString() + " ";
        }
//        Pair[] pairArr = new Pair[arr.size()];
//        for (int i = 0; i < arr.size(); i++) {
//            pairArr[i] = arr.get(i);
//            System.out.println(pairArr[i].toString());
//        }
        documentAndIndexList.set(arr.toArray(new Pair[arr.size()]));
        System.out.println(documentAndIndexList.toString());
        context.write(key, documentAndIndexList);
	}
}
