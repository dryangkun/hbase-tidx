package com.github.dryangkun.hbase.tidx;

import org.apache.hadoop.hbase.Cell;

import java.util.HashSet;
import java.util.Set;

public class TxDelete extends TxCell {

    private final Set<Integer> mIndexes = new HashSet<Integer>();

    public TxDelete(byte[] tRow, Cell tCell, int mIndex) {
        super(tRow, tCell);
        mIndexes.add(mIndex);
    }

    public void addMIndex(int mIndex) {
        mIndexes.add(mIndex);
    }

    public Set<Integer> getMIndexes() {
        return mIndexes;
    }
}
