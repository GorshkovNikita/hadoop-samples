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

    public String getDocName() {
        return docName;
    }

    public int getIndex() {
        return index;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Pair)) return false;

        Pair pair = (Pair) o;

        if (index != pair.index) return false;
        if (docName != null ? !docName.equals(pair.docName) : pair.docName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = docName != null ? docName.hashCode() : 0;
        result = 31 * result + index;
        return result;
    }
}
