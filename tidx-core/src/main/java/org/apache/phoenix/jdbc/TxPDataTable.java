package org.apache.phoenix.jdbc;

import com.github.dryangkun.hbase.tidx.TxUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.schema.*;

import java.sql.SQLException;
import java.util.List;

public class TxPDataTable {

    private final String dataTableName;
    private final PTable dataTable;

    public TxPDataTable(String dataTableName, PhoenixConnection conn) throws SQLException {
        PhoenixStatement stmt = (PhoenixStatement) conn.createStatement();
        try {
            String sql = "select * from " + dataTableName;
            PhoenixStatement.CompilableStatement compilableStatement = stmt.parseStatement(sql);
            ScanPlan plan = compilableStatement.compilePlan(stmt, Sequence.ValueOp.RESERVE_SEQUENCE);

            this.dataTableName = dataTableName.toUpperCase();
            this.dataTable = plan.getTableRef().getTable();

            List<PTable> indexes = this.dataTable.getIndexes();
            if (indexes == null || indexes.isEmpty()) {
                throw new SQLException("data table:" + dataTableName + " has no index");
            }
        } finally {
            try { stmt.close(); } catch(SQLException e) {}
        }
    }

    private static boolean isLocalIndex(PTable table) {
        return table.getType() == PTableType.INDEX &&
                table.getIndexType() == PTable.IndexType.LOCAL;
    }

    public PTable getDataTable() {
        return dataTable;
    }

    public PTable getLocalIndexTable(String indexName) throws SQLException {
        List<PTable> indexes = dataTable.getIndexes();
        indexName = indexName.toUpperCase();
        for (PTable index : indexes) {
            if (isLocalIndex(index)) {
                if (index.getTableName().getString().equals(indexName)) {
                    return index;
                }
            }
        }
        throw new SQLException("dataTable:" + dataTableName + " has no local index:" + indexName);
    }

    public HTable getLocalIndexHTable(PhoenixConnection conn) throws SQLException {
        TableName indexTableName = TxUtils.getIndexTableName(Bytes.toBytes(dataTableName));
        return (HTable) conn.getQueryServices().getTable(indexTableName.getName());
    }

    public HTable getHTable(PhoenixConnection conn) throws SQLException {
        return (HTable) conn.getQueryServices().getTable(Bytes.toBytes(dataTableName));
    }

    public PColumn getColumn(String columnName) throws SQLException {
        columnName = columnName.toUpperCase();
        try {
            return dataTable.getColumn(columnName);
        } catch (ColumnNotFoundException e) {
            throw new SQLException("dataTable:" + dataTableName + " has no column:" + columnName);
        }
    }

    public ColumnReference getColumnReference(String columnName) throws SQLException {
        PColumn column = getColumn(columnName);
        byte[] family = column.getFamilyName().getBytes();
        byte[] qualifier = column.getName().getBytes();
        return new ColumnReference(family, qualifier);
    }
}