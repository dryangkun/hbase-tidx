package com.github.dryangkun.hbase.tidx;

public class TxConstants {

    public static final String DOBSERVER_ARG_TIME_COLUMN = "tx.time.column";
    public static final String DOBSERVER_ARG_PHOENIX_INDEX_ID = "tx.phoenix.index.id";

    public static final byte[] PHOENIX_INDEX_FAMILY = "0".getBytes();
    public static final byte[] PHOENIX_INDEX_QUALIFIER = "_0".getBytes();

    public static final String IOBSERVER_CONF_SCAN_DATA = "tx.scan.data";
    public static final String IOBSERVER_CONF_SCAN_DATA_GET = "tx.scan.data.get";

    public static final byte[] TRUE_BYTES = new byte[] { 1 };
}
