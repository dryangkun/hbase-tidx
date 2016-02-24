package com.github.dryangkun.hbase.tidx;

import org.apache.hadoop.hbase.Cell;

public class TxPut extends TxCell {

    private int maxMIndex;
    private long maxMTimestamp;

    public TxPut(byte[] tRow, Cell tCell, int mIndex, long mTimestamp) {
        super(tRow, tCell);
        this.maxMIndex = mIndex;
        this.maxMTimestamp = mTimestamp;
    }

    public void merge(int mIndex, long mTimestamp) {
        if (maxMTimestamp < mTimestamp) {
            this.maxMIndex = mIndex;
            this.maxMTimestamp = mTimestamp;
        } else if (maxMTimestamp == mTimestamp && this.maxMIndex < mIndex) {
            this.maxMIndex = mIndex;
        }
    }

    public int getMaxMIndex() {
        return maxMIndex;
    }

    public long getMaxMTimestamp() {
        return maxMTimestamp;
    }
}
