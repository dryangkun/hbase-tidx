package com.github.dryangkun.hbase.tidx;

import org.apache.hadoop.hbase.client.Get;

public class TxConstants {

    public static final String OBSERVER_TIME_COL = "tx.time.col";
    public static final String OBSERVER_PHOENIX_INDEX_ID = "tx.pidx.id";

    public static final byte[] PHOENIX_INDEX_FAMILY = "0".getBytes();
    public static final byte[] PHOENIX_INDEX_QUALIFIER = "_0".getBytes();

    public static final String INDEX_SCAN_ATTRIBUTES = "tx.iscan";
    public static final String INDEX_SCAN_ATTRIBUTES_DATA_GET = "tx.iscan.dget";
    public static final String INDEX_SCAN_ATTRIBUTES_TIME_CHECK = "tx.iscan.tcheck";

    public static final byte[] VIRTUAL_INDEX_TIME_FAMILY = "0".getBytes();
    public static final byte[] VIRTUAL_INDEX_TIME_QUALIFIER = "^T".getBytes();

    public static final byte[] TRUE_BYTES = new byte[] { 1 };
    public static final long INVALID_TIME = -10;
    public static final long INFINITY_TIME = -1;
    public static final int DEFAULT_SCAN_CACHING = 100;

    public static final String MR_CONF_START_TIME = "tx.mr.s";
    public static final String MR_CONF_END_TIME = "tx.mr.e";
    public static final String MR_CONF_PHOENIX_INDEX_ID = "tx.mr.pidx.id";
    public static final String MR_CONF_TIME_CHECK = "tx.mr.tcheck";
    public static final String MR_CONF_DATA_GET = "tx.mr.dget";
    public static final String MR_CONF_TABLE = "tx.mr.table";
}
