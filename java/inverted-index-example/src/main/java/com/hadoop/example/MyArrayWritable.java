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
        String result = "";
        for (Pair pair : pairs) {
            result += pair.toString() + " ";
        }
        return result;
    }
}
