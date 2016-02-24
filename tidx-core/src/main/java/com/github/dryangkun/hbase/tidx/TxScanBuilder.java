package com.github.dryangkun.hbase.tidx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TxScanBuilder {

    public static final long INFINITY_TIME = -1;
    public static final int DEFAULT_SCAN_CACHING = 100;

    private Get dataGet;
    private long startTime = INFINITY_TIME;
    private long endTime = INFINITY_TIME;
    private short phoenixIndexId = -1;

    private int caching = DEFAULT_SCAN_CACHING;

    public TxScanBuilder setDataGet(Get get) {
        dataGet = get;
        return this;
    }

    public TxScanBuilder setStartTime(long startTime) {
        if (startTime <= 0 && startTime != INFINITY_TIME) {
            throw new IllegalArgumentException("");
        }
        this.startTime = startTime;
        return this;
    }

    public TxScanBuilder setEndTime(long endTime) {
        if (endTime <= 0 && endTime != INFINITY_TIME) {
            throw new IllegalArgumentException("");
        }
        this.endTime = endTime;
        return this;
    }

    public TxScanBuilder setCaching(int caching) {
        if (caching <= 0) {
            throw new IllegalArgumentException("");
        }
        this.caching = caching;
        return this;
    }

    public TxScanBuilder setPhoenixIndexId(short phoenixIndexId) {
        this.phoenixIndexId = phoenixIndexId;
        return this;
    }

    public Scan build(byte[] regionStartKey, byte[] regionEndKey) throws IOException {
        if (dataGet == null) {
            throw new IOException("DataGet must be assigned before build");
        }
        if (startTime == INFINITY_TIME && endTime == INFINITY_TIME) {
            throw new IOException("StartTime or EndTime must be assigned at least before build");
        }
        if (phoenixIndexId == -1) {
            throw new IOException("PhoenixIndexId must be assigned before build");
        }

        TxIndexRowBuilder indexRowBuilder = new TxIndexRowBuilder(
                regionStartKey, regionEndKey, phoenixIndexId);
        byte[] start = startTime != INFINITY_TIME ? indexRowBuilder.build(startTime) : HConstants.EMPTY_START_ROW;
        byte[] end = endTime != INFINITY_TIME ? indexRowBuilder.build(endTime) : HConstants.EMPTY_END_ROW;

        Scan scan = new Scan(start, end);
        scan.setMaxVersions(1);
        scan.setCaching(caching);
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.addColumn(TxConstants.PHOENIX_INDEX_FAMILY, TxConstants.PHOENIX_INDEX_QUALIFIER);
        scan.setAttribute(TxConstants.IOBSERVER_CONF_SCAN_DATA, TxConstants.TRUE_BYTES);
        scan.setAttribute(TxConstants.IOBSERVER_CONF_SCAN_DATA_GET, ProtobufUtil.toGet(dataGet).toByteArray());

        return scan;
    }

    public List<Scan> build(HBaseAdmin admin, byte[] tableName) throws IOException {
        List<HRegionInfo> ris = admin.getTableRegions(TableName.valueOf(tableName));
        List<Scan> scans = new ArrayList<Scan>(ris.size());

        for (HRegionInfo ri : ris) {
            scans.add(build(ri.getStartKey(), ri.getEndKey()));
        }
        return scans;
    }

    public List<Scan> build(Configuration conf, byte[] tableName) throws IOException {
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(conf);
            return build(admin, tableName);
        } finally {
            if (admin != null) {
                try { admin.close(); } catch(IOException e) {}
            }
        }
    }
}
