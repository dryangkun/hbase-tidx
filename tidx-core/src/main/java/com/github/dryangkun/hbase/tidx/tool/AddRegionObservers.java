package com.github.dryangkun.hbase.tidx.tool;

import com.github.dryangkun.hbase.tidx.TxConstants;
import com.github.dryangkun.hbase.tidx.TxDataRegionObserver;
import com.github.dryangkun.hbase.tidx.TxIndexRegionObserver;
import com.github.dryangkun.hbase.tidx.TxUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class AddRegionObservers {

    public void add(String jdbcUrl, String dataTable, String indexName, String timeCol) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(jdbcUrl);

        short phoenixIndexId;
        byte[] dataHTableName;
        try {
            phoenixIndexId = (new GetPhoenixIndexId()).get(conn, dataTable, indexName);
            PTable dataPTable = PhoenixRuntime.getTable(conn, dataTable);
            dataHTableName = dataPTable.getPhysicalName().getBytes();
        } finally {
            if (conn != null) {
                try { conn.close(); } catch(SQLException e) {}
            }
        }

        Configuration conf = TxUtils.createConfiguration(jdbcUrl);
        boolean dataTableDisable = false;
        boolean indexTableDisable = false;
        byte[] indexHTableName = TxUtils.getIndexTableName(dataHTableName).getName();

        Map<String, String> arguments = new HashMap<>();
        arguments.put(TxConstants.OBSERVER_TIME_COL, timeCol);
        arguments.put(TxConstants.OBSERVER_PHOENIX_INDEX_ID, "" + phoenixIndexId);
        String className;

        HBaseAdmin admin = new HBaseAdmin(conf);
        try {
            if (admin.isTableEnabled(dataHTableName)) {
                admin.disableTable(dataHTableName);
                dataTableDisable = true;
            }

            HTableDescriptor dataTableDesc = admin.getTableDescriptor(dataHTableName);
            className = TxDataRegionObserver.class.getName();
            if (!dataTableDesc.hasCoprocessor(className)) {
                dataTableDesc.addCoprocessor(className, null, Coprocessor.PRIORITY_USER, arguments);
                admin.modifyTable(dataHTableName, dataTableDesc);
                admin.enableTable(dataHTableName);
                dataTableDisable = false;
            }

            if (admin.isTableEnabled(indexHTableName)) {
                admin.disableTable(indexHTableName);
                indexTableDisable = true;
            }

            HTableDescriptor indexTableDesc = admin.getTableDescriptor(indexHTableName);
            className = TxIndexRegionObserver.class.getName();
            if (!indexTableDesc.hasCoprocessor(className)) {
                indexTableDesc.addCoprocessor(className, null, Coprocessor.PRIORITY_USER, arguments);
                admin.modifyTable(indexHTableName, indexTableDesc);
                admin.enableTable(indexHTableName);
                indexTableDisable = false;
            }
        } finally {
            if (dataTableDisable) {
                try {
                    admin.enableTable(dataHTableName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (indexTableDisable) {
                try {
                    admin.enableTable(indexHTableName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try { admin.close(); } catch(IOException e) {}
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(ToolUtils.OPTION_HELP);
        options.addOption(ToolUtils.OPTION_JDBC_URL);
        options.addOption(ToolUtils.OPTION_DATA_TABLE);
        options.addOption(ToolUtils.OPTION_INDEX_NAME);
        options.addOption(null, "time-col", true, "update-time family:qualifier in data table");
        CommandLine commandLine = ToolUtils.parseCommandLine(options, args, AddRegionObservers.class);

        String jdbcUrl = commandLine.getOptionValue(ToolUtils.OPTION_JDBC_URL_KEY);
        String dataTable = commandLine.getOptionValue(ToolUtils.OPTION_DATA_TABLE_KEY);
        String indexName = commandLine.getOptionValue(ToolUtils.OPTION_INDEX_NAME_KEY);
        String timeCol = commandLine.getOptionValue("time-col");
        (new AddRegionObservers()).add(jdbcUrl, dataTable, indexName, timeCol);
    }
}
