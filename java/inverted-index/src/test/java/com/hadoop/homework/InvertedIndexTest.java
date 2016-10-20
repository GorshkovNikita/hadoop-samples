package com.hadoop.homework;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;
import static org.junit.Assert.*;

public class InvertedIndexTest {
	MapDriver<LongWritable, Text, Text, Pair> mapDriver;
	ReduceDriver<Text, Pair, Text, PairList> reduceDriver;
	
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
                new Text("Doc=it is simple test"));
		mapDriver.addOutput(new Text("it"), new Pair("Doc", 0));
        mapDriver.addOutput(new Text("is"), new Pair("Doc", 3));
        mapDriver.addOutput(new Text("simple"), new Pair("Doc", 6));
        mapDriver.addOutput(new Text("test"), new Pair("Doc", 13));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
        List<Pair> list = new ArrayList<Pair>();
		list.add(new Pair("Doc1", 0));
        list.add(new Pair("Doc2", 10));
        PairList documentAndIndexList = new PairList(Pair.class);
        reduceDriver.setInput(new Text("test"), list);
        documentAndIndexList.set(new Pair[]{ list.get(0), list.get(1)});
        reduceDriver.withOutput(new Text("test"), documentAndIndexList);
		reduceDriver.runTest();
    }

    @Test
    public void testPairListEquals() {
        PairList documentAndIndexList1 = new PairList(Pair.class);
        documentAndIndexList1.set(new Pair[]{ new Pair("Doc1", 1), new Pair("Doc2", 10) });
        PairList documentAndIndexList2 = new PairList(Pair.class);
        documentAndIndexList2.set(new Pair[]{ new Pair("Doc1", 1), new Pair("Doc2", 10) });
        assertEquals(documentAndIndexList1, documentAndIndexList2);
    }
}
