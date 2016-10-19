package com.hadoop.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;

public class InvertedIndexTest {
	MapDriver<LongWritable, Text, Text, Pair> mapDriver;
	ReduceDriver<Text, Pair, Text, MyArrayWritable> reduceDriver;
	
	@Before
	public void setUp() {
		InvertedIndexMapper mapper = new InvertedIndexMapper();
		InvertedIndexReducer reducer = new InvertedIndexReducer();
		mapDriver = MapDriver.newMapDriver();
		mapDriver.setMapper(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(),
                new Text("D[0]=it is simple test"));
		mapDriver.addOutput(new Text("it"), new Pair("D[0]", 0));
        mapDriver.addOutput(new Text("is"), new Pair("D[0]", 3));
        mapDriver.addOutput(new Text("simple"), new Pair("D[0]", 6));
        mapDriver.addOutput(new Text("test"), new Pair("D[0]", 13));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		List<Pair> list = new ArrayList<Pair>();
		list.add(new Pair("T[0]", 0));
        list.add(new Pair("T[1]", 10));
        MyArrayWritable documentAndIndexList = new MyArrayWritable(Pair.class);
        reduceDriver.setInput(new Text("test"), list);
        documentAndIndexList.set(new Pair[]{new Pair("T[0]", 0), new Pair("T[1]", 10)});
        reduceDriver.withOutput(new Text("test"), documentAndIndexList);
        for (Pair pair: documentAndIndexList.get()) {
            System.out.println(pair.toString());
        }
		reduceDriver.runTest();
	}
}
