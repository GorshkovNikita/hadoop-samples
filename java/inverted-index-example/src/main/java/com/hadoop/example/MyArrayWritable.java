package com.hadoop.example;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author Никита
 */
public class MyArrayWritable extends ArrayWritable {
    public MyArrayWritable(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    @Override
    public Pair[] get() {
        return (Pair[]) super.get();
    }

    @Override
    public String toString() {
        Pair[] pairs = get();
        System.out.println("length = " + pairs.length);
        String result = "";
        for (Pair pair : pairs) {
            System.out.println(pair.toString());;
            result += pair.toString();
        }
        return result;
    }
}
