package com.github.dryangkun.hbase.tindex;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

public class TxCell {

    private final byte[] tRow;
    private final long tTimestamp;
    private final long tValue;

    public TxCell(byte[] tRow, Cell tCell) {
        this.tRow = tRow;
        if (tCell != null) {
            this.tTimestamp = tCell.getTimestamp();
            this.tValue = TxUtils.getTime(tCell);
        } else {
            this.tTimestamp = -100;
            this.tValue = -100;
        }
    }

    public boolean isEmpty() {
        return tTimestamp == -100;
    }

    public byte[] getTRow() {
        return tRow;
    }

    public long getTTimestamp() {
        return tTimestamp;
    }

    public long getTValue() {
        return tValue;
    }

    @Override
    public String toString() {
        return "TxCell{" +
                "tRow=" + Bytes.toHex(tRow) +
                ", tTimestamp=" + tTimestamp +
                ", tValue=" + tValue +
                '}';
    }
}
