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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class AddRegionObservers {

    public void add(String jdbcUrl, String dataTable, String indexName, String timeCol) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        jdbcUrl = ToolUtils.formatPhoenixJdbcUrl(jdbcUrl);
        dataTable = SchemaUtil.normalizeIdentifier(dataTable);
        indexName = SchemaUtil.normalizeIdentifier(indexName);

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
                System.out.println("Start Disable DataTable - " + Bytes.toString(dataHTableName));
                admin.disableTable(dataHTableName);
                dataTableDisable = true;
                System.out.println("Finish Disable DataTable");
            }

            HTableDescriptor dataTableDesc = admin.getTableDescriptor(dataHTableName);
            className = TxDataRegionObserver.class.getName();
            if (!dataTableDesc.hasCoprocessor(className)) {
                System.out.println("Start Add RegionObserver - " + className);
                dataTableDesc.addCoprocessor(className, null, Coprocessor.PRIORITY_USER, arguments);
                admin.modifyTable(dataHTableName, dataTableDesc);
                admin.enableTable(dataHTableName);
                dataTableDisable = false;
                System.out.println("Finish Add RegionObserver");
            } else {
                System.out.println("DataTable already exists RegionObserver - " + className);
            }

            if (admin.isTableEnabled(indexHTableName)) {
                System.out.println("Start Disable IndexTable - " + Bytes.toString(indexHTableName));
                admin.disableTable(indexHTableName);
                indexTableDisable = true;
                System.out.println("Finish Disable IndexTable");
            }

            HTableDescriptor indexTableDesc = admin.getTableDescriptor(indexHTableName);
            className = TxIndexRegionObserver.class.getName();
            if (!indexTableDesc.hasCoprocessor(className)) {
                System.out.println("Start Add RegionObserver - " + className);
                indexTableDesc.addCoprocessor(className, null, Coprocessor.PRIORITY_USER, arguments);
                admin.modifyTable(indexHTableName, indexTableDesc);
                admin.enableTable(indexHTableName);
                indexTableDisable = false;
                System.out.println("Finish Add RegionObserver");
            } else {
                System.out.println("IndexTable already exists RegionObserver - " + className);
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
