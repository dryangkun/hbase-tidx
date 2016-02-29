package com.github.dryangkun.hbase.tidx.hive;

import com.github.dryangkun.hbase.tidx.TxConstants;
import com.github.dryangkun.hbase.tidx.TxScanBuilder;
import com.github.dryangkun.hbase.tidx.TxUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.*;

public class TxHiveTableInputFormatUtil {

    private static final Log LOG = LogFactory.getLog(TxHiveTableInputFormatUtil.class);

    public static ColumnMappings.ColumnMapping appendIndexPredicateAnalyzer(IndexPredicateAnalyzer analyzer,
                                                                            ColumnMappings columnMappings,
                                                                            JobConf jobConf) {
        int timeColIndex = -1;
        try {
            timeColIndex = HBaseSerDe.getTxTimeColumnIndex(columnMappings, jobConf);
        } catch (SerDeException e) {
            LOG.warn("get time column index fail", e);
        }

        if (timeColIndex != -1) {
            ColumnMappings.ColumnMapping timeColMapping = columnMappings.getColumnsMapping()[timeColIndex];
            appendIndexPredicateAnalyzer(analyzer, timeColMapping.columnName);
            return timeColMapping;
        } else {
            LOG.warn("no time column");
            return null;
        }
    }

    public static void appendIndexPredicateAnalyzer(IndexPredicateAnalyzer analyzer, String timeColName) {
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan");
        analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");
        analyzer.allowColumnName(timeColName);
    }

    private static Get createDataGet(JobConf jobConf,
                                     ColumnMappings columnMappings) throws IOException {
        Get dataGet = TxUtils.createDataGet();
        dataGet.setMaxVersions(1);

        Set<String> addedFamilies = new HashSet<>();
        List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(jobConf);
        boolean empty = true;

        ColumnMappings.ColumnMapping[] columnsMapping = columnMappings.getColumnsMapping();
        for (int i : readColIDs) {
            ColumnMappings.ColumnMapping colMap = columnsMapping[i];
            if (colMap.hbaseRowKey) {
                continue;
            }

            if (colMap.qualifierName == null) {
                dataGet.addFamily(colMap.familyNameBytes);
                addedFamilies.add(colMap.familyName);
            } else {
                if (!addedFamilies.contains(colMap.familyName)) {
                    dataGet.addColumn(colMap.familyNameBytes, colMap.qualifierNameBytes);
                }
            }
            empty = false;
        }
        if (empty) {
            for (ColumnMappings.ColumnMapping colMap : columnMappings) {
                if (colMap.hbaseRowKey) {
                    continue;
                }

                if (colMap.qualifierName == null) {
                    dataGet.addFamily(colMap.familyNameBytes);
                } else {
                    dataGet.addColumn(colMap.familyNameBytes, colMap.qualifierNameBytes);
                }
            }
        }
        return dataGet;
    }

    private static Map<String, List<IndexSearchCondition>> createSearchConditions(
            JobConf jobConf, ColumnMappings columnMappings,
            int iTimeColumn, String[] columnNames) throws IOException {

        String filterExprSerialized = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        if (filterExprSerialized == null) {
            return null;
        }
        ExprNodeGenericFuncDesc filterExpr = Utilities.deserializeExpression(filterExprSerialized);

        int iKey = columnMappings.getKeyIndex();
        boolean isKeyBinary = HiveHBaseInputFormatUtil.getStorageFormatOfKey(
                columnMappings.getKeyMapping().mappingSpec,
                jobConf.get(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE, "string"));

        String keyColName = columnNames[iKey];
        String keyColType = jobConf.get(serdeConstants.LIST_COLUMN_TYPES).split(",")[iKey];
        boolean isKeyComparable = isKeyBinary || keyColType.equalsIgnoreCase("string");

        String timeColName = columnNames[iTimeColumn];
        IndexPredicateAnalyzer analyzer =
                HiveHBaseTableInputFormat.newIndexPredicateAnalyzer(keyColName, keyColType, isKeyComparable);
        appendIndexPredicateAnalyzer(analyzer, timeColName);

        List<IndexSearchCondition> searchConditions = new ArrayList<>();
        ExprNodeDesc residualPredicate = analyzer.analyzePredicate(filterExpr, searchConditions);

        if (residualPredicate != null) {
            LOG.debug("Ignoring residual predicate " + residualPredicate.getExprString());
        }
        return decomposeSearchConditions(searchConditions);
    }

    private static Map<String, List<IndexSearchCondition>> decomposeSearchConditions(List<IndexSearchCondition> searchConditions) {
        Map<String, List<IndexSearchCondition>> result = new HashMap<>();
        for (IndexSearchCondition condition : searchConditions) {
            List<IndexSearchCondition> conditions = result.get(condition.getColumnDesc().getColumn());
            if (conditions == null) {
                conditions = new ArrayList<>();
                result.put(condition.getColumnDesc().getColumn(), conditions);
            }
            conditions.add(condition);
        }
        return result;
    }

    public static List<InputSplit> getSplits(JobConf jobConf, int numSplits,
                                         ColumnMappings columnMappings, int iTimeColumn,
                                         String hbaseTableName) throws IOException {
        String[] columnNames = jobConf.get(serdeConstants.LIST_COLUMNS).split(",");
        Map<String, List<IndexSearchCondition>> searchConditions =
                createSearchConditions(jobConf, columnMappings, iTimeColumn, columnNames);
        if (searchConditions == null) {
            return null;
        }

        String keyColName = columnNames[columnMappings.getKeyIndex()];
        String timeColName = columnNames[iTimeColumn];
        List<IndexSearchCondition> keyConditions = searchConditions.get(keyColName);
        List<IndexSearchCondition> timeConditions = searchConditions.get(timeColName);

        if ((keyConditions != null && !keyConditions.isEmpty()) ||
            (timeConditions == null || timeConditions.isEmpty())) {
            return null;
        }

        long[] times = parseTimeConditions(timeConditions);
        if (times[0] == TxConstants.INFINITY_TIME && times[1] == TxConstants.INFINITY_TIME) {
            LOG.warn("getSplits: StartTime and EndTime both infinity");
            return null;
        }
        int phoenixIndexId = jobConf.getInt(HBaseSerDe.TX_HIVE_PHOENIX_INDEX_ID, -1);
        if (phoenixIndexId == -1) {
            LOG.warn("getSplits: " + HBaseSerDe.TX_HIVE_PHOENIX_INDEX_ID + " not exists in job conf");
            return null;
        }
        boolean timeCheck = jobConf.getBoolean(HBaseSerDe.TX_HIVE_TIME_CHECK, false);
        int scanCache = jobConf.getInt(HBaseSerDe.HBASE_SCAN_CACHE, -1);
        boolean scanCacheBlocks = jobConf.getBoolean(HBaseSerDe.HBASE_SCAN_CACHEBLOCKS, false);

        Get dataGet = createDataGet(jobConf, columnMappings);
        dataGet.setCacheBlocks(scanCacheBlocks);
        if (LOG.isDebugEnabled()) {
            LOG.debug("getSplits: data get -> " + dataGet);
        }

        TxScanBuilder scanBuilder = new TxScanBuilder();
        scanBuilder.setPhoenixIndexId(phoenixIndexId)
                .setStartTime(times[0])
                .setEndTime(times[1])
                .setTimeCheck(timeCheck)
                .setDataGet(dataGet)
                .setCaching(scanCache);

        TableName indexTableName = TxUtils.getIndexTableName(Bytes.toBytes(hbaseTableName));
        List<InputSplit> splits = new ArrayList<>();

        Configuration conf = HBaseConfiguration.create(jobConf);
        try (HTable indexTable = new HTable(conf, indexTableName)) {
            List<Scan> scans = scanBuilder.build(conf, indexTableName.getName());
            RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(indexTable);
            Pair<byte[][], byte[][]> keys = indexTable.getStartEndKeys();

            for (int i = 0; i < keys.getFirst().length; i++) {
                Scan scan = scans.get(i);
                scan.setCacheBlocks(scanCacheBlocks);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getSplits: scan -> " + i + " -> " + scan);
                }

                HRegionLocation hregionLocation =
                        indexTable.getRegionLocation(keys.getFirst()[i], false);
                String regionHostname = hregionLocation.getHostname();
                HRegionInfo regionInfo = hregionLocation.getRegionInfo();
                long regionSize = sizeCalculator.getRegionSize(regionInfo.getRegionName());

                TableSplit split = new TableSplit(
                        indexTable.getName(), scan, scan.getStartRow(), scan.getStopRow(),
                        regionHostname, regionSize);
                splits.add(split);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getSplits: split -> " + i + " -> " + split);
                }
            }
        }
        return splits;
    }

    private static void setupTimeRange(
            Get get, List<IndexSearchCondition> conditions) throws IOException {
        long start = 0;
        long end = Long.MAX_VALUE;
        for (IndexSearchCondition sc : conditions) {
            long timestamp = getTimestampVal(sc);
            String comparisonOp = sc.getComparisonOp();
            if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual".equals(comparisonOp)) {
                start = timestamp;
                end = timestamp + 1;
            } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan".equals(comparisonOp)) {
                end = timestamp;
            } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan"
                    .equals(comparisonOp)) {
                start = timestamp;
            } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan"
                    .equals(comparisonOp)) {
                start = timestamp + 1;
            } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan"
                    .equals(comparisonOp)) {
                end = timestamp + 1;
            } else {
                throw new IOException(comparisonOp + " is not a supported comparison operator");
            }
        }
        get.setTimeRange(start, end);
    }

    private static long[] parseTimeConditions(List<IndexSearchCondition> tConditions) throws IOException {
        long startTime = TxConstants.INFINITY_TIME, endTime = TxConstants.INFINITY_TIME;
        for (IndexSearchCondition sc : tConditions) {
            long timestamp = getTimestampVal(sc);
            String comparisonOp = sc.getComparisonOp();

            if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual".equals(comparisonOp)) {
                startTime = timestamp;
                endTime = startTime + 1;
            } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan"
                    .equals(comparisonOp)) {
                endTime = timestamp + 1;
            } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan".equals(comparisonOp)) {
                endTime = timestamp;
            } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan"
                    .equals(comparisonOp)) {
                startTime = timestamp;
            } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan"
                    .equals(comparisonOp)) {
                startTime = timestamp + 1;
            } else {
                throw new IOException(comparisonOp + " is not a supported comparison operator");
            }
        }
        return new long[]{startTime, endTime};
    }

    private static long getTimestampVal(IndexSearchCondition sc) throws IOException {
        long timestamp;
        try {
            ExprNodeConstantEvaluator eval = new ExprNodeConstantEvaluator(sc.getConstantDesc());
            ObjectInspector inspector = eval.initialize(null);
            Object value = eval.evaluate(null);
            if (inspector instanceof LongObjectInspector) {
                timestamp = ((LongObjectInspector) inspector).get(value);
            } else {
                PrimitiveObjectInspector primitive = (PrimitiveObjectInspector) inspector;
                timestamp = PrimitiveObjectInspectorUtils.getTimestamp(value, primitive).getTime();
            }
        } catch (HiveException e) {
            throw new IOException(e);
        }
        return timestamp;
    }

    public static RecordReader<ImmutableBytesWritable, Result> createRecordReader(
            InputSplit split, TaskAttemptContext context, JobConf jobConf) throws IOException {
        TableSplit tSplit = (TableSplit) split;
        LOG.info("Input split length: " + StringUtils.humanReadableInt(tSplit.getLength()) + " bytes.");

        final TableRecordReader trr = new TableRecordReader();
        final HTable table = new HTable(HBaseConfiguration.create(jobConf), tSplit.getTable());

        Scan sc = new Scan(tSplit.getScan());
        sc.setStartRow(tSplit.getStartRow());
        sc.setStopRow(tSplit.getEndRow());

        trr.setScan(sc);
        trr.setHTable(table);
        return new RecordReader<ImmutableBytesWritable, Result>() {

            @Override
            public void close() throws IOException {
                trr.close();
                table.close();
            }

            @Override
            public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
                return trr.getCurrentKey();
            }

            @Override
            public Result getCurrentValue() throws IOException, InterruptedException {
                return trr.getCurrentValue();
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return trr.getProgress();
            }

            @Override
            public void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException,
                    InterruptedException {
                trr.initialize(inputsplit, context);
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return trr.nextKeyValue();
            }
        };
    }
}
