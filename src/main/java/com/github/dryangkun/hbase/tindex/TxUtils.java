package com.github.dryangkun.hbase.tindex;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

public class TxUtils {

    public static long getTime(Cell cell) {
        return Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }
}
