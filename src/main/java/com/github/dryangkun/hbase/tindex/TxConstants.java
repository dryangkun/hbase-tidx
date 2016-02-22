package com.github.dryangkun.hbase.tindex;

public class TxConstants {

    public static final String DATA_OBSERV_ARG_TIME_COLUMN = "tx.time.column";
    public static final String DATA_OBSERV_ARG_PHOENIX_INDEX_ID = "tx.phoenix.index.id";

    public static final byte[] PHOENIX_INDEX_FAMILY = "0".getBytes();
    public static final byte[] PHOENIX_INDEX_QUALIFIER = "_0".getBytes();

    public static final byte[] EMPTY_BYTES = new byte[0];
}
