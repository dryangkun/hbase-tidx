package com.github.dryangkun.hbase.tidx;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
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

    private static final ThreadLocal<RegionHolder> THREAD_REGION_HOLDER = new ThreadLocal<RegionHolder>();

    private static class RegionHolder {
        final ReentrantLock lock = new ReentrantLock();
        final String indexRegionName;
        final TxIndexRowCodec indexRowCodec;

        volatile boolean inPreBatchMutate = false;

        Map<ImmutableBytesPtr, TxPut> puts;
        Map<ImmutableBytesPtr, TxDelete> deletes;

        public RegionHolder(String indexRegionName, TxIndexRowCodec indexRowCodec) {
            this.indexRegionName = indexRegionName;
            this.indexRowCodec = indexRowCodec;
        }
    }

    private final Map<Long, RegionHolder> regionHolders = new ConcurrentHashMap<Long, RegionHolder>();

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
                                 TxCell cell, TxIndexRowCodec indexRowCodec) throws IOException {
        if (!cell.isEmpty()) {
            byte[] indexRow = indexRowCodec.encode(cell.getTValue(), cell.getTRow());
            Delete indexDelete = new Delete(indexRow);
            indexDelete.deleteColumn(
                    TxConstants.PHOENIX_INDEX_FAMILY,
                    TxConstants.PHOENIX_INDEX_QUALIFIER,
                    cell.getTTimestamp());
            indexRegion.delete(indexDelete);
        }
    }

    private RegionHolder getRegionHolder(HRegionInfo ri, ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
        RegionHolder regionHolder = regionHolders.get(ri.getRegionId());
        if (regionHolder == null) {
            synchronized (regionHolders) {
                if ((regionHolder = regionHolders.get(ri.getRegionId())) == null) {
                    LOG.debug("initialize RegionHolder for the region " + ri.getRegionNameAsString());
                    HRegion indexRegion = (HRegion) IndexUtil.getIndexRegion(c.getEnvironment());
                    if (indexRegion == null) {
                        return null;
                    }
                    HRegionInfo iri = indexRegion.getRegionInfo();

                    regionHolder = new RegionHolder(iri.getEncodedName(),
                            new TxIndexRowCodec(iri.getStartKey(), iri.getEndKey(), phoenixIndexId));
                    regionHolders.put(ri.getRegionId(), regionHolder);
                }
            }
        }
        return regionHolder;
    }

    private void initBatchMutation(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
        RegionHolder regionHolder = THREAD_REGION_HOLDER.get();
        if (regionHolder == null) {
            HRegion region = (HRegion) c.getEnvironment().getRegion();
            HRegionInfo ri = region.getRegionInfo();

            regionHolder = getRegionHolder(ri, c);
            if (regionHolder == null) {
                return;
            }

            RegionServerServices rss = c.getEnvironment().getRegionServerServices();
            if (rss.getFromOnlineRegions(regionHolder.indexRegionName) == null) {
                return;
            }

            regionHolder.lock.lock();
            THREAD_REGION_HOLDER.set(regionHolder);
        }
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        initBatchMutation(c);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
        initBatchMutation(c);
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        RegionHolder regionHolder = THREAD_REGION_HOLDER.get();
        if (regionHolder == null) {
            return;
        }

        HRegion region = (HRegion) c.getEnvironment().getRegion();
        HRegionInfo ri = region.getRegionInfo();

        RegionServerServices rss = c.getEnvironment().getRegionServerServices();
        if (rss.getFromOnlineRegions(regionHolder.indexRegionName) == null) {
            return;
        }

        Map<ImmutableBytesPtr, TxPut> puts = new HashMap<ImmutableBytesPtr, TxPut>(miniBatchOp.size());
        Map<ImmutableBytesPtr, TxDelete> deletes = new HashMap<ImmutableBytesPtr, TxDelete>(miniBatchOp.size());

        regionHolder.inPreBatchMutate = true;
        try {
            for (int i = 0; i < miniBatchOp.size(); i++) {
                OperationStatus s = miniBatchOp.getOperationStatus(i);
                if (s.getOperationStatusCode() != HConstants.OperationStatusCode.NOT_RUN) {
                    continue;
                }

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
                            deletes.put(row, txDelete);
                        } else {
                            txDelete.addMIndex(i);
                        }
                    }
                }
            }
        } finally {
            regionHolder.inPreBatchMutate = false;
        }
        regionHolder.puts = puts;
        regionHolder.deletes = deletes;
    }

    @Override
    public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp, boolean success) throws IOException {
        if (THREAD_REGION_HOLDER.get() == null) {
            return;
        }
        RegionHolder regionHolder = THREAD_REGION_HOLDER.get();

        RegionServerServices rss = c.getEnvironment().getRegionServerServices();
        HRegion indexRegion = (HRegion) rss.getFromOnlineRegions(regionHolder.indexRegionName);
        if (indexRegion == null) {
            return;
        }
        HRegionInfo iri = indexRegion.getRegionInfo();

        TxIndexRowCodec indexRowCodec = regionHolder.indexRowCodec;
        Map<ImmutableBytesPtr, TxPut> puts = regionHolder.puts;
        Map<ImmutableBytesPtr, TxDelete> deletes = regionHolder.deletes;

        regionHolder.puts = null;
        regionHolder.deletes = null;

        for (TxPut txPut : puts.values()) {
            OperationStatus s = miniBatchOp.getOperationStatus(txPut.getMaxMIndex());
            if (s.getOperationStatusCode() == HConstants.OperationStatusCode.SUCCESS) {
                Put p = (Put) miniBatchOp.getOperation(txPut.getMaxMIndex());
                Cell cell = p.get(timeFamily, timeQualifier).get(0);

                if (cell.getTimestamp() < txPut.getTTimestamp()) {
                    continue;
                }
                deleteTimeIndex(indexRegion, txPut, indexRowCodec);

                byte[] indexRow = indexRowCodec.encode(TxUtils.getTime(cell), p.getRow());
                {
                    byte[] b = indexRowCodec.encode(txPut.getTValue(), txPut.getTRow());
                    LOG.info("compare " + Bytes.toStringBinary(b) + "@" + txPut.getTTimestamp() +
                            " to " + Bytes.toStringBinary(indexRow) + "@" + cell.getTimestamp());
                }
                Put indexPut = new Put(indexRow);
                indexPut.add(
                        TxConstants.PHOENIX_INDEX_FAMILY,
                        TxConstants.PHOENIX_INDEX_QUALIFIER,
                        cell.getTimestamp(),
                        HConstants.EMPTY_BYTE_ARRAY);
                indexPut.setDurability(p.getDurability());
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
            boolean _success = false;
            for (Integer mIndex : txDelete.getMIndexes()) {
                OperationStatus s = miniBatchOp.getOperationStatus(mIndex);
                if (s.getOperationStatusCode() == HConstants.OperationStatusCode.SUCCESS) {
                    _success = true;
                    break;
                }
            }
            if (_success) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("deleting the cell " + txDelete + " from index region " + iri.getRegionNameAsString());
                }
                deleteTimeIndex(indexRegion, txDelete, indexRowCodec);
            } else {
                LOG.warn("operation " + txDelete.getMIndexes() + " insuccess for " + txDelete);
            }
        }
    }

    @Override
    public void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> c, HRegion.Operation op) throws IOException {
        RegionHolder regionHolder = THREAD_REGION_HOLDER.get();
        if (regionHolder != null) {
            if (regionHolder.inPreBatchMutate) {
                return;
            }
            regionHolder.puts = null;
            regionHolder.deletes = null;
            THREAD_REGION_HOLDER.set(null);
            regionHolder.lock.unlock();
        }
    }
}
