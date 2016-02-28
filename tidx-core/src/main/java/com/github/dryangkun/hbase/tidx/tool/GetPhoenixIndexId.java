package com.github.dryangkun.hbase.tidx.tool;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.TxPDataTable;
import org.apache.phoenix.schema.PTable;

import java.sql.DriverManager;
import java.sql.SQLException;

public class GetPhoenixIndexId {

    public short get(String jdbcUrl, String dTableName, String iTableName) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(jdbcUrl);
        try {
            return get(conn, dTableName, iTableName);
        } finally {
            if (conn != null) {
                try { conn.close(); } catch(SQLException e) {}
            }
        }
    }

    public short get(PhoenixConnection conn, String dTableName, String iTableName) throws Exception {
        TxPDataTable dataTable = new TxPDataTable("t1", conn);
        PTable indexPTable = dataTable.getLocalIndexTable(iTableName);
        return indexPTable.getViewIndexId();
    }
}
