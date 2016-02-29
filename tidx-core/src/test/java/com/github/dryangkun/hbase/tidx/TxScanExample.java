package com.github.dryangkun.hbase.tidx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class TxScanExample {

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        TxScanBuilder scanBuilder = new TxScanBuilder();

        Get dataGet = TxUtils.createDataGet();
        dataGet.addColumn("0".getBytes(), "A".getBytes());
        dataGet.addColumn("0".getBytes(), "T".getBytes());

        scanBuilder.setStartTime(100)
                   .setEndTime(104)
                   .setPhoenixIndexId((short) 0)
                   .setDataGet(dataGet);

        TableName indexTableName = TxUtils.getIndexTableName("T1".getBytes());
        List<Scan> scans = scanBuilder.build(conf, indexTableName.getName());
        HTable indexTable = new HTable(conf, indexTableName);

        for (Scan scan : scans) {
            System.out.println("scan = " + scan);

            ResultScanner scanner = indexTable.getScanner(scan);
            try {
                for (Result result : scanner) {
                    System.out.println("row - " + Bytes.toString(result.getRow()));
                    System.out.println("\t0:A  - " + Bytes.toString(result.getValue("0".getBytes(), "A".getBytes())));
                    System.out.println("\t0:T  - " + Bytes.toLong(result.getValue("0".getBytes(), "T".getBytes())));
                    System.out.println("\t0:^T - " + Bytes.toLong(
                            result.getValue(TxConstants.VIRTUAL_INDEX_TIME_FAMILY,
                                    TxConstants.VIRTUAL_INDEX_TIME_QUALIFIER)));
                }
            } finally {
                scanner.close();
            }
        }

        indexTable.close();
    }
}
