package com.github.dryangkun.hbase.tindex;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;

public class TxRegionObserver extends BaseScannerRegionObserver {

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return false;
    }

    @Override
    protected RegionScanner doPostScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Scan scan, RegionScanner regionScanner) throws Throwable {
        return null;
    }
}
