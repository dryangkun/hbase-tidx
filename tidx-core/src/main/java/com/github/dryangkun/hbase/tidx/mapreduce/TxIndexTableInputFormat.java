package com.github.dryangkun.hbase.tidx.mapreduce;

import com.github.dryangkun.hbase.tidx.TxConstants;
import com.github.dryangkun.hbase.tidx.TxScanBuilder;
import com.github.dryangkun.hbase.tidx.TxUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormatBase;

import java.util.List;

public class TxIndexTableInputFormat extends MultiTableInputFormatBase implements Configurable {

    private Configuration conf = null;

    public void setConf(Configuration configuration) {
        conf = configuration;

        long startTime = conf.getLong(TxConstants.MR_CONF_START_TIME, TxConstants.INVALID_TIME);
        long endTime = conf.getLong(TxConstants.MR_CONF_END_TIME, TxConstants.INVALID_TIME);
        int phoenixIndexId = conf.getInt(TxConstants.MR_CONF_PHOENIX_INDEX_ID, -1);
        boolean timeCheck = conf.getBoolean(TxConstants.MR_CONF_TIME_CHECK, false);

        String tableNameStr = conf.get(TxConstants.MR_CONF_TABLE);
        if (TxUtils.isEmpty(tableNameStr)) {
            throw new IllegalArgumentException(TxConstants.MR_CONF_TABLE + " must be assigned in conf");
        }
        byte[] tableName = TableName.valueOf(tableNameStr).getName();

        String dataGetStr = conf.get(TxConstants.MR_CONF_DATA_GET);
        if (TxUtils.isEmpty(dataGetStr)) {
            throw new IllegalArgumentException(TxConstants.MR_CONF_DATA_GET + " must be assigned in conf");
        }

        try {
            Get dataGet = TxUtils.convertStringToGet(dataGetStr);

            TxScanBuilder scanBuilder = new TxScanBuilder();
            scanBuilder.setCaching(0)
                    .setStartTime(startTime)
                    .setEndTime(endTime)
                    .setPhoenixIndexId(phoenixIndexId)
                    .setTimeCheck(timeCheck)
                    .setDataGet(dataGet);

            List<Scan> scans = scanBuilder.build(conf, tableName);
            for (Scan scan : scans) {
                scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName);
            }
            setScans(scans);
        } catch (Exception e) {
            throw new RuntimeException("build scans fail", e);
        }
    }

    public Configuration getConf() {
        return conf;
    }
}
