package com.github.dryangkun.hbase.tidx.tool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.TxPhoenixUtils;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.DriverManager;
import java.sql.SQLException;

public class GetPhoenixIndexId {

    public short get(String jdbcUrl, String dataTable, String indexName) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        jdbcUrl = ToolUtils.formatPhoenixJdbcUrl(jdbcUrl);
        PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(jdbcUrl);
        try {
            return get(conn, dataTable, indexName);
        } finally {
            if (conn != null) {
                try { conn.close(); } catch(SQLException e) {}
            }
        }
    }

    public short get(PhoenixConnection conn, String dataTable, String indexName) throws Exception {
        dataTable = SchemaUtil.normalizeIdentifier(dataTable);
        indexName = SchemaUtil.normalizeIdentifier(indexName);
        PTable dataPTable = PhoenixRuntime.getTable(conn, dataTable);
        PTable indexPTable = TxPhoenixUtils.getLocalIndexPTable(dataPTable, indexName);
        return indexPTable.getViewIndexId();
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(ToolUtils.OPTION_HELP);
        options.addOption(ToolUtils.OPTION_JDBC_URL);
        options.addOption(ToolUtils.OPTION_DATA_TABLE);
        options.addOption(ToolUtils.OPTION_INDEX_NAME);
        CommandLine commandLine = ToolUtils.parseCommandLine(options, args, GetPhoenixIndexId.class);

        String jdbcUrl = commandLine.getOptionValue(ToolUtils.OPTION_JDBC_URL_KEY);
        String dataTable = commandLine.getOptionValue(ToolUtils.OPTION_DATA_TABLE_KEY);
        String indexName = commandLine.getOptionValue(ToolUtils.OPTION_INDEX_NAME_KEY);
        short phoenixIndexId = (new GetPhoenixIndexId()).get(jdbcUrl, dataTable, indexName);
        System.out.println("Phoenix Index Id: " + phoenixIndexId + " (note: negative is normal)");
    }
}
