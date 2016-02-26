package com.github.dryangkun.hbase.tidx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TxScanBuilder {

    private Get dataGet;
    private long startTime = TxConstants.INFINITY_TIME;
    private long endTime = TxConstants.INFINITY_TIME;
    private short phoenixIndexId = -1;
    private boolean timeCheck = false;

    private int caching = TxConstants.DEFAULT_SCAN_CACHING;

    public TxScanBuilder setDataGet(Get get) {
        dataGet = get;
        return this;
    }

    public TxScanBuilder setStartTime(long startTime) {
        if (startTime <= 0 && startTime != TxConstants.INFINITY_TIME) {
            throw new IllegalArgumentException("start time invalid - " + startTime);
        }
        this.startTime = startTime;
        return this;
    }

    public TxScanBuilder setEndTime(long endTime) {
        if (endTime <= 0 && endTime != TxConstants.INFINITY_TIME) {
            throw new IllegalArgumentException("end time invalid - " + endTime);
        }
        this.endTime = endTime;
        return this;
    }

    public TxScanBuilder setCaching(int caching) {
        if (caching < 0) {
            caching = -1;
        }
        this.caching = caching;
        return this;
    }

    public TxScanBuilder setPhoenixIndexId(short phoenixIndexId) {
        if (phoenixIndexId < 0) {
            throw new IllegalArgumentException("phoenix index id invalid - " + phoenixIndexId);
        }
        this.phoenixIndexId = phoenixIndexId;
        return this;
    }

    public TxScanBuilder setPhoenixIndexId(int phoenixIndexId) {
        if (phoenixIndexId < Short.MIN_VALUE || phoenixIndexId > Short.MAX_VALUE) {
            throw new IllegalArgumentException("phoenix index id must be short - " + phoenixIndexId);
        }
        return setPhoenixIndexId((short) phoenixIndexId);
    }

    public TxScanBuilder setTimeCheck(boolean timeCheck) {
        this.timeCheck = timeCheck;
        return this;
    }

    public Scan build(byte[] regionStartKey, byte[] regionEndKey) throws IOException {
        if (dataGet == null) {
            throw new IOException("DataGet must be assigned before build");
        }
        if (startTime == TxConstants.INFINITY_TIME && endTime == TxConstants.INFINITY_TIME) {
            throw new IOException("StartTime or EndTime must be assigned at least before build");
        }
        if (phoenixIndexId == -1) {
            throw new IOException("PhoenixIndexId must be assigned before build");
        }

        TxIndexRowCodec indexRowCodec = new TxIndexRowCodec(regionStartKey, regionEndKey, phoenixIndexId);
        byte[] start = startTime != TxConstants.INFINITY_TIME ? indexRowCodec.encode(startTime) : HConstants.EMPTY_START_ROW;
        byte[] end = endTime != TxConstants.INFINITY_TIME ? indexRowCodec.encode(endTime) : HConstants.EMPTY_END_ROW;

        Scan scan = new Scan(start, end);
        scan.setMaxVersions(1);
        scan.setCaching(caching);
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.addColumn(TxConstants.PHOENIX_INDEX_FAMILY, TxConstants.PHOENIX_INDEX_QUALIFIER);
        scan.setAttribute(TxConstants.IOB_CONF_ISCAN, TxConstants.TRUE_BYTES);
        scan.setAttribute(TxConstants.IOB_CONF_ISCAN_DATA_GET, TxUtils.convertGetToBytes(dataGet));
        if (timeCheck) {
            scan.setAttribute(TxConstants.IOB_CONF_ISCAN_TIME_CHECK, TxConstants.TRUE_BYTES);
        }

        return scan;
    }

    public List<Scan> build(HBaseAdmin admin, byte[] indexTableName) throws IOException {
        List<HRegionInfo> ris = admin.getTableRegions(TableName.valueOf(indexTableName));
        List<Scan> scans = new ArrayList<Scan>(ris.size());

        for (HRegionInfo ri : ris) {
            scans.add(build(ri.getStartKey(), ri.getEndKey()));
        }
        return scans;
    }

    public List<Scan> build(Connection conn, byte[] indexTableName) throws IOException {
        Admin admin = conn.getAdmin();
        try {
            List<HRegionInfo> ris = admin.getTableRegions(TableName.valueOf(indexTableName));
            List<Scan> scans = new ArrayList<Scan>(ris.size());

            for (HRegionInfo ri : ris) {
                scans.add(build(ri.getStartKey(), ri.getEndKey()));
            }
            return scans;
        } finally {
            if (admin != null) {
                try { admin.close(); } catch(IOException e) {}
            }
        }
    }

    public List<Scan> build(Configuration conf, byte[] indexTableName) throws IOException {
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            return build(conn, indexTableName);
        } finally {
            try { conn.close(); } catch(IOException e) {}
        }
    }
}
