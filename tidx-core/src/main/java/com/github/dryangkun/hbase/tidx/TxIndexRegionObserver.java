package com.github.dryangkun.hbase.tidx;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ServerUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TxIndexRegionObserver extends BaseRegionObserver {

    private static final Log LOG = LogFactory.getLog(TxIndexRegionObserver.class);

    private static class Context {
        HRegion dRegion;
        TxIndexRowCodec indexRowCodec;
        Get dGet;
        boolean timeCheck;

        byte[] timeFamily;
        byte[] timeQualifier;
    }

    private byte[] timeFamily;
    private byte[] timeQualifier;
    private short phoenixIndexId;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        Configuration conf = e.getConfiguration();

        byte[][] items = TxUtils.parseTimeColumn(conf, LOG);
        timeFamily = items[0];
        timeQualifier = items[1];

        phoenixIndexId = TxUtils.parsePhoenixIndexId(conf, LOG);
    }

    protected boolean isRegionObserverFor(Scan scan) {
        return scan.getAttribute(TxConstants.INDEX_SCAN_ATTRIBUTES) != null;
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
        if (!isRegionObserverFor(scan)) {
            return s;
        }
        Context context = new Context();

        HRegion iRegion = (HRegion) c.getEnvironment().getRegion();
        context.dRegion = (HRegion) IndexUtil.getDataRegion(c.getEnvironment());
        if (context.dRegion == null) {
            return s;
        }

        byte[] bytes = scan.getAttribute(TxConstants.INDEX_SCAN_ATTRIBUTES_DATA_GET);
        context.dGet = bytes != null ? TxUtils.convertBytesToGet(bytes) : null;
        if (context.dGet == null) {
            LOG.warn(TxConstants.INDEX_SCAN_ATTRIBUTES_DATA_GET + " is null in scan attribute");
            return s;
        }

        context.timeCheck = scan.getAttribute(TxConstants.INDEX_SCAN_ATTRIBUTES_TIME_CHECK) != null;
        context.timeFamily = timeFamily;
        context.timeQualifier = timeQualifier;

        HRegionInfo iri = iRegion.getRegionInfo();
        context.indexRowCodec = new TxIndexRowCodec(iri.getStartKey(), iri.getEndKey(), phoenixIndexId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("iscan open for region " + iri.getRegionNameAsString());
        }
        return getWrappedScanner(c, s, context);
    }

    private static Get cloneDataGet(byte[] dataRow, Get dataGet) throws IOException {
        Get get = new Get(dataRow);

        get.setFilter(dataGet.getFilter());
        get.setCacheBlocks(dataGet.getCacheBlocks());
        get.setMaxVersions(dataGet.getMaxVersions());
        get.setMaxResultsPerColumnFamily(dataGet.getMaxResultsPerColumnFamily());
        get.setRowOffsetPerColumnFamily(dataGet.getRowOffsetPerColumnFamily());
        get.setTimeRange(dataGet.getTimeRange().getMin(), dataGet.getTimeRange().getMax());
        get.setCheckExistenceOnly(dataGet.isCheckExistenceOnly());
        get.setClosestRowBefore(dataGet.isClosestRowBefore());
        get.getFamilyMap().putAll(dataGet.getFamilyMap());

        Iterator i$ = dataGet.getAttributesMap().entrySet().iterator();
        while(i$.hasNext()) {
            Map.Entry attr = (Map.Entry)i$.next();
            get.setAttribute((String) attr.getKey(), (byte[]) attr.getValue());
        }

        return get;
    }

    private static RegionScanner getWrappedScanner(final ObserverContext<RegionCoprocessorEnvironment> c,
                                                   final RegionScanner s,
                                                   final Context context) throws IOException {

        final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        return new RegionScanner() {

            public HRegionInfo getRegionInfo() {
                return s.getRegionInfo();
            }

            public boolean isFilterDone() throws IOException {
                return s.isFilterDone();
            }

            public boolean reseek(byte[] row) throws IOException {
                return s.reseek(row);
            }

            public long getMaxResultSize() {
                return s.getMaxResultSize();
            }

            public long getMvccReadPoint() {
                return s.getMvccReadPoint();
            }

            public int getBatch() {
                return s.getBatch();
            }

            public boolean nextRaw(List<Cell> result) throws IOException {
                try {
                    boolean next = s.nextRaw(result);
                    fillDataResult(result, context);
                    return next;
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegionInfo().getRegionNameAsString(), t);
                    return false;
                }
            }

            public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
                try {
                    boolean next = s.nextRaw(result, scannerContext);
                    fillDataResult(result, context);
                    return next;
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegionInfo().getRegionNameAsString(), t);
                    return false;
                }
            }

            public boolean next(List<Cell> result) throws IOException {
                try {
                    return s.next(result);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegionInfo().getRegionNameAsString(), t);
                    return false;
                }
            }

            public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
                try {
                    return s.next(result, scannerContext);
                } catch (Throwable t) {
                    ServerUtil.throwIOException(c.getEnvironment().getRegionInfo().getRegionNameAsString(), t);
                    return false;
                }
            }

            public void close() throws IOException {
                s.close();
            }
        };
    }

    private static void fillDataResult(List<Cell> result, Context context) throws IOException {
        if (result.isEmpty()) {
            return;
        }

        Cell cell = result.get(0);
        if (LOG.isDebugEnabled()) {
            LOG.debug("index cell " + cell);
        }
        result.clear();

        byte[] indexRow = CellUtil.cloneRow(cell);
        long time;
        byte[] dataRow;
        try {
            Pair<Long, byte[]> pair = context.indexRowCodec.decode(indexRow);
            time = pair.getFirst();
            dataRow = pair.getSecond();
        } catch (Exception e) {
            LOG.error("decode data row fail from index row " + Bytes.toStringBinary(indexRow), e);
            return;
        }

        Get newDGet = cloneDataGet(dataRow, context.dGet);
        List<Cell> dResult = context.dRegion.get(newDGet, false);
        if (!dResult.isEmpty()) {
            if (context.timeCheck) {
                for (Cell c : dResult) {
                    if (TxUtils.equalCell(c, context.timeFamily, context.timeQualifier)) {
                        try {
                            long dataTime = TxUtils.getTime(c);
                            if (dataTime != time) {
                                LOG.warn("index time " + time + " not equal data time " + dataTime + " for data row " + Bytes.toStringBinary(dataRow));
                                return;
                            }
                        } catch (Exception e) {
                            LOG.warn("invalid data time for data row " + Bytes.toStringBinary(dataRow));
                            return;
                        }
                        break;
                    }
                }
                result.addAll(dResult);
            } else {
                result.addAll(dResult);
                Cell tCell = CellUtil.createCell(dataRow,
                        TxConstants.VIRTUAL_INDEX_TIME_FAMILY, TxConstants.VIRTUAL_INDEX_TIME_QUALIFIER,
                        cell.getTimestamp(), KeyValue.Type.Maximum.getCode(), Bytes.toBytes(time));
                result.add(tCell);
            }
        }
    }
}
