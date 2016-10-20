package com.hadoop.homework;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * Список пар {@link com.hadoop.homework.Pair}, который поступает
 * на выход reduce шага
 * @author Никита
 */
public class PairList extends ArrayWritable {
    public PairList() {
        super(Pair.class);
    }

    public PairList(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    @Override
    public Writable[] get() {
        return (Writable[]) super.get();
    }

    @Override
    public String toString() {
        Writable[] pairs = get();
        String result = "";
        for (Writable pair : pairs) {
            result += pair.toString() + " ";
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PairList)) return false;

        PairList that = (PairList) o;

        Writable[] thisPairs = this.get();
        Writable[] thatPairs = that.get();
        for (int i = 0; i < thisPairs.length; i++) {
            if (!thisPairs[i].equals(thatPairs[i])) return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return get().length;
    }

}
