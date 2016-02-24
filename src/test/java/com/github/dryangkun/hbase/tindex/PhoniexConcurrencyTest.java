package com.github.dryangkun.hbase.tindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PhoniexConcurrencyTest {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

        ExecutorService service = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 2; i++) {
            final int j = i;
            service.submit(new Runnable() {
                public void run() {
                    Connection conn = null;
                    try {
                        System.out.println("start " + j);
                        conn = DriverManager.getConnection("jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-unsecure");
                        conn.setAutoCommit(true);

                        PreparedStatement stmt = conn.prepareStatement("upsert into T2 values(?,?)");

                        Random r = new Random();
                        for (int i = 0; i < 100; i++) {
                            long k = 100 + r.nextInt(4) + j;
                            String row = "c" + r.nextInt(4);

                            stmt.setString(1, row);
                            stmt.setLong(2, k);
                            stmt.execute();
                        }
                        System.out.println("stop " + j);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    } finally {
                        if (conn != null) {
                            try { conn.close(); } catch(SQLException e) {}
                        }
                    }

                }
            });
        }

        service.shutdown();
        while (!service.awaitTermination(1000L, TimeUnit.MILLISECONDS));
    }
}
