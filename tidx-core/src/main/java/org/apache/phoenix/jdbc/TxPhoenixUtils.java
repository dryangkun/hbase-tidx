package org.apache.phoenix.jdbc;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;

import java.sql.SQLException;
import java.util.List;

public class TxPhoenixUtils {

    public static PhoenixEmbeddedDriver.ConnectionInfo createConnectionInfo(String phoenixJdbcUrl) throws SQLException {
        return PhoenixEmbeddedDriver.ConnectionInfo.create(phoenixJdbcUrl);
    }

    public static boolean isLocalIndex(PTable table) {
        return table.getType() == PTableType.INDEX &&
                table.getIndexType() == PTable.IndexType.LOCAL;
    }

    public static PTable getLocalIndexPTable(PTable dataPTable, String indexName) throws SQLException {
        List<PTable> indexes = dataPTable.getIndexes();
        indexName = indexName.toUpperCase();
        for (PTable index : indexes) {
            if (isLocalIndex(index)) {
                if (index.getTableName().getString().equals(indexName)) {
                    return index;
                }
            }
        }
        throw new SQLException("phoenix data table:" + dataPTable.getName() + " has no local index:" + indexName);
    }

    public static HTable getHTable(PhoenixConnection conn, PTable pTable) throws SQLException {
        return (HTable) conn.getQueryServices().getTable(pTable.getName().getBytes());
    }
}
