package com.github.dryangkun.hbase.tidx;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;

public class TxRegionObserver extends BaseScannerRegionObserver {

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return scan.getAttribute(TxConstants.IOBSERVER_CONF_SCAN_DATA) != null;
    }

    @Override
    protected RegionScanner doPostScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Scan scan, RegionScanner regionScanner) throws Throwable {
        return null;
    }
}
