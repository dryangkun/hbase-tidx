package com.github.dryangkun.hbase.tidx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TxConcurrencyTest {

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        ExecutorService service = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 2; i++) {
            final int j = i;
            service.submit(new Runnable() {
                public void run() {
                    HTable hTable = null;
                    try {
                        System.out.println("start " + j);
                        hTable = new HTable(conf, "T1".getBytes());
                        hTable.setAutoFlush(false);
                        Random r = new Random();
                        System.out.println("" + j + " - " + System.currentTimeMillis());
                        for (int i = 0; i < 100; i++) {
                            long k = 100 + r.nextInt(4) + j;

                            byte[] row = ("zzzzz" + r.nextInt(4)).getBytes();
                            System.out.println("" + j + " - " + Bytes.toString(row));
                            Put p = new Put(row);
                            p.add("0".getBytes(), "T".getBytes(), Bytes.toBytes(k));
                            p.add("0".getBytes(), "A".getBytes(), Bytes.toBytes("www" + i));
                            hTable.put(p);
                            if (i % 20 == 19) {
                                hTable.flushCommits();
                            }
                        }
                        hTable.flushCommits();
                        System.out.println("" + j + " - " + System.currentTimeMillis());
                        System.out.println("stop " + j);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    } finally {
                        if (hTable != null) {
                            try { hTable.close(); } catch(IOException e) {}
                        }
                    }

                }
            });
        }

        service.shutdown();
        while (!service.awaitTermination(1000L, TimeUnit.MILLISECONDS));
    }
}
