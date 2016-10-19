package com.hadoop.example;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Никита
 */
public class Pair implements Writable {
    private String docName = "";
    private int index = 0;

    public Pair() {}

    public Pair(String docName, int index) {
        this.index = index;
        this.docName = docName;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        docName = WritableUtils.readString(in);
        index = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, docName);
        out.writeInt(index);
    }

//    public void merge(Pair other) {
//        this.docName += other.docName;
//        this.index += other.index;
//    }

    @Override
    public String toString() {
        return "(" + this.docName + " " + this.index + ")";
    }
}
