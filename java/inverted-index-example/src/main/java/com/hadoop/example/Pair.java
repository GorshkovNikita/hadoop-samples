package com.hadoop.example;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author Никита
 */
public class Pair implements Writable {
    private String docName;
    private Integer index;
    private int countBytes;

    public Pair() {
    }

    public Pair(String docName, Integer index) {
        this.docName = docName;
        this.index = index;
        this.countBytes = docName.getBytes().length;
    }

    public void write(DataOutput out) throws IOException {
        out.writeBytes(docName);
        out.writeInt(index);
    }

    public void readFields(DataInput in) throws IOException {
        byte[] bytes = new byte[countBytes];
        for (int i = 0; i < countBytes; i++) {
            bytes[i] = in.readByte();
        }
        this.docName = new String(bytes, StandardCharsets.UTF_8);
        this.index = in.readInt();
    }

    public static Pair read(DataInput in) throws IOException {
        Pair w = new Pair();
        w.readFields(in);
        return w;
    }
}
