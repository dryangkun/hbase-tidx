package com.github.dryangkun.hbase.tindex;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.IndexUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class TxDataRegionObserver extends BaseRegionObserver {

    private static final Log LOG = LogFactory.getLog(TxDataRegionObserver.class);

    private static final ThreadLocal<Boolean> PASS = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    private static class RegionHolder {
        final ReentrantLock lock = new ReentrantLock();
        final String indexRegionName;
        final TxIndexRowBuilder indexRowBuilder;

        volatile boolean stopUnLock = false;
        Map<ImmutableBytesPtr, TxPut> puts;
        Map<ImmutableBytesPtr, TxDelete> deletes;

        public RegionHolder(String indexRegionName, TxIndexRowBuilder indexRowBuilder) {
            this.indexRegionName = indexRegionName;
            this.indexRowBuilder = indexRowBuilder;
        }
    }

    private byte[] timeFamily;
    private byte[] timeQualifier;
    private short phoenixIndexId;

    private final Map<Long, RegionHolder> regionHolders = new ConcurrentHashMap<Long, RegionHolder>();

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        Configuration conf = e.getConfiguration();

        {
            String timeColumn = conf.get(TxConstants.DATA_OBSERV_ARG_TIME_COLUMN);
            LOG.debug("data observer argument " + TxConstants.DATA_OBSERV_ARG_TIME_COLUMN +
                    " = " + timeColumn);
            if (TxUtils.isEmpty(timeColumn)) {
                throw new IOException("data observer argument " +
                        TxConstants.DATA_OBSERV_ARG_TIME_COLUMN + " missed");
            }

            String[] items = timeColumn.split(":", 2);
            if (items.length != 2) {
                throw new IOException(TxConstants.DATA_OBSERV_ARG_TIME_COLUMN + "=" +
                        timeColumn + " invalid(family:qualifier)");
            }
            timeFamily = items[0].getBytes();
            timeQualifier = items[1].getBytes();
        }

        {
            String indexIdStr = conf.get(TxConstants.DATA_OBSERV_ARG_PHOENIX_INDEX_ID);
            LOG.debug("data observer argument " + TxConstants.DATA_OBSERV_ARG_PHOENIX_INDEX_ID +
                    " = " + indexIdStr);
            if (TxUtils.isEmpty(indexIdStr)) {
                throw new IOException("data observer argument " +
                        TxConstants.DATA_OBSERV_ARG_PHOENIX_INDEX_ID + " missed");
            }

            short indexId;
            try {
                indexId = Short.parseShort(indexIdStr);
            } catch (NumberFormatException ex) {
                throw new IOException("data observer argument " +
                        TxConstants.DATA_OBSERV_ARG_PHOENIX_INDEX_ID + " is invalid", ex);
            }
            phoenixIndexId = indexId;
        }
    }

    private boolean checkTimeColumn(Put put) {
        return put.has(timeFamily, timeQualifier);
    }

    private boolean checkTimeColumn(Delete delete) {
        NavigableMap<byte [], List<Cell>> m = delete.getFamilyCellMap();
        if (m == null || m.isEmpty()) {
            return true;
        }

        List<Cell> cells = m.get(timeFamily);
        if (cells == null) {
            return false;
        }

        for (Cell cell : cells) {
            if (Bytes.equals(timeQualifier, 0, timeQualifier.length,
                    cell.getQualifierArray(),
                    cell.getQualifierOffset(),
                    cell.getQualifierLength())) {
                return true;
            }
        }
        return false;
    }

    private Cell getTimeRawCell(HRegion region, byte[] row) throws IOException {
        Get g = new Get(row);
        g.addColumn(timeFamily, timeQualifier);
        g.setMaxVersions(1);
        List<Cell> cells = region.get(g, false);
        return cells != null && !cells.isEmpty() ? cells.get(0) : null;
    }

    private void deleteTimeIndex(HRegion indexRegion,
                                 TxCell cell, TxIndexRowBuilder indexRowBuilder) throws IOException {
        if (!cell.isEmpty()) {
            byte[] indexRow = indexRowBuilder.build(cell.getTValue(), cell.getTRow());
            Delete indexDelete = new Delete(indexRow);
            indexDelete.deleteColumn(
                    TxConstants.PHOENIX_INDEX_FAMILY,
                    TxConstants.PHOENIX_INDEX_QUALIFIER,
                    cell.getTTimestamp());
            indexRegion.delete(indexDelete);
        }
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        HRegion region = (HRegion) c.getEnvironment().getRegion();
        HRegionInfo ri = region.getRegionInfo();
        long regionId = ri.getRegionId();

        RegionHolder regionHolder = regionHolders.get(regionId);
        if (regionHolder == null) {
            synchronized (regionHolders) {
                if ((regionHolder = regionHolders.get(regionId)) == null) {
                    LOG.debug("initialize RegionHolder for the region " + ri.getRegionNameAsString());

                    HRegion indexRegion = (HRegion) IndexUtil.getIndexRegion(c.getEnvironment());
                    HRegionInfo iri = indexRegion.getRegionInfo();

                    regionHolder = new RegionHolder(iri.getEncodedName(),
                            new TxIndexRowBuilder(iri.getStartKey(), iri.getEndKey(), phoenixIndexId));
                    regionHolders.put(regionId, regionHolder);
                }
            }
        }
        RegionServerServices rss = c.getEnvironment().getRegionServerServices();
        if (rss.getFromOnlineRegions(regionHolder.indexRegionName) == null) {
            return;
        }

        regionHolder.lock.lock();
        if (LOG.isDebugEnabled()) {
            LOG.debug("locked the region " + ri.getRegionNameAsString());
        }

        Map<ImmutableBytesPtr, TxPut> puts = new HashMap<ImmutableBytesPtr, TxPut>(miniBatchOp.size());
        Map<ImmutableBytesPtr, TxDelete> deletes = new HashMap<ImmutableBytesPtr, TxDelete>(miniBatchOp.size());

        PASS.set(false);
        regionHolder.stopUnLock = true;
        try {
            for (int i = 0; i < miniBatchOp.size(); i++) {
                Mutation m = miniBatchOp.getOperation(i);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("mutation " + m + " at preBatchMutate for region " + ri.getRegionNameAsString());
                }

                ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
                if (m instanceof Put) {
                    Put p = (Put) m;
                    if (checkTimeColumn(p)) {
                        if (deletes.containsKey(row)) {
                            throw new IOException("can't put and delete the same row " +
                                    Bytes.toHex(m.getRow()) + " int this batch mutations");
                        }

                        TxPut txPut = puts.get(row);
                        if (txPut == null) {
                            Cell tCell = getTimeRawCell(region, p.getRow());
                            txPut = new TxPut(p.getRow(), tCell, i, p.getTimeStamp());
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("get the old time " + txPut + " for put");
                            }

                            puts.put(row, txPut);
                        } else {
                            txPut.merge(i, p.getTimeStamp());
                        }
                    }
                } else if (m instanceof Delete) {
                    Delete d = (Delete) m;
                    if (checkTimeColumn(d)) {
                        if (puts.containsKey(row)) {
                            throw new IOException("can't put and delete the same row " +
                                    Bytes.toHex(m.getRow()) + " int this batch mutations");
                        }

                        TxDelete txDelete = deletes.get(row);
                        if (txDelete == null) {
                            Cell tCell = getTimeRawCell(region, d.getRow());
                            txDelete = new TxDelete(d.getRow(), tCell, i);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("get the old time " + txDelete + " for delete");
                            }

                            deletes.put(row, txDelete);
                        } else {
                            txDelete.addMIndex(i);
                        }
                    }
                }
            }
        } finally {
            regionHolder.stopUnLock = false;
        }

        if (puts.isEmpty() && deletes.isEmpty()) {
            PASS.set(true);
            regionHolder.lock.unlock();
        } else {
            regionHolder.puts = puts;
            regionHolder.deletes = deletes;
        }
    }

    @Override
    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        if (PASS.get()) {
            return;
        }

        HRegion region = (HRegion) c.getEnvironment().getRegion();
        HRegionInfo ri = region.getRegionInfo();
        long regionId = ri.getRegionId();

        RegionHolder regionHolder = regionHolders.get(regionId);
        if (regionHolder == null) {
            return;
        }

        RegionServerServices rss = c.getEnvironment().getRegionServerServices();
        HRegion indexRegion = (HRegion) rss.getFromOnlineRegions(regionHolder.indexRegionName);
        if (indexRegion == null) {
            return;
        }
        HRegionInfo iri = indexRegion.getRegionInfo();

        TxIndexRowBuilder indexRowBuilder = regionHolder.indexRowBuilder;
        Map<ImmutableBytesPtr, TxPut> puts = regionHolder.puts;
        Map<ImmutableBytesPtr, TxDelete> deletes = regionHolder.deletes;

        regionHolder.puts = null;
        regionHolder.deletes = null;

        for (TxPut txPut : puts.values()) {
            OperationStatus s = miniBatchOp.getOperationStatus(txPut.getMaxMIndex());
            if (s.getOperationStatusCode() == HConstants.OperationStatusCode.SUCCESS) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("deleting the cell " + txPut + " from index region " + iri.getRegionNameAsString());
                }
                deleteTimeIndex(indexRegion, txPut, indexRowBuilder);

                Put p = (Put) miniBatchOp.getOperation(txPut.getMaxMIndex());
                Cell cell = p.get(timeFamily, timeQualifier).get(0);
                long value = TxUtils.getTime(cell);
                byte[] indexRow = indexRowBuilder.build(value, p.getRow());

                Put indexPut = new Put(indexRow);
                indexPut.add(
                        TxConstants.PHOENIX_INDEX_FAMILY,
                        TxConstants.PHOENIX_INDEX_QUALIFIER,
                        cell.getTimestamp(),
                        TxConstants.EMPTY_BYTES);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("putting the cell " + indexPut + " to index region " + iri.getRegionNameAsString());
                }
                indexRegion.put(indexPut);
            } else {
                LOG.warn("operation " + miniBatchOp.getOperation(txPut.getMaxMIndex()) +
                        " insuccess for " + txPut);
            }
        }

        for (TxDelete txDelete : deletes.values()) {
            boolean success = false;
            for (Integer mIndex : txDelete.getMIndexes()) {
                OperationStatus s = miniBatchOp.getOperationStatus(mIndex);
                if (s.getOperationStatusCode() == HConstants.OperationStatusCode.SUCCESS) {
                    success = true;
                    break;
                }
            }
            if (success) {
                deleteTimeIndex(indexRegion, txDelete, indexRowBuilder);
            } else {
                LOG.warn("operation " + txDelete.getMIndexes() + " insuccess for " + txDelete);
            }
        }
    }

    @Override
    public void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> c, HRegion.Operation op) throws IOException {
        HRegion region = (HRegion) c.getEnvironment().getRegion();
        long regionId = region.getRegionInfo().getRegionId();

        RegionHolder regionHolder = regionHolders.get(regionId);
        if (regionHolder != null && regionHolder.lock.isLocked() && !regionHolder.stopUnLock) {
            if (regionHolder.puts != null) {
                regionHolder.puts = null;
            }
            if (regionHolder.deletes != null) {
                regionHolder.deletes = null;
            }

            regionHolder.lock.unlock();
            if (LOG.isDebugEnabled()) {
                LOG.debug("unlock the region " + region.getRegionInfo().getRegionNameAsString());
            }
        }
    }
}
