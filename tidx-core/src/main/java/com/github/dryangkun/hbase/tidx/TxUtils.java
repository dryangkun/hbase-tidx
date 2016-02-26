package com.github.dryangkun.hbase.tidx;

import com.sun.org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.MetaDataUtil;

import java.io.IOException;

public class TxUtils {

    public static long getTime(Cell cell) {
        return Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    public static byte[] convertGetToBytes(Get get) throws IOException {
        return ProtobufUtil.toGet(get).toByteArray();
    }

    public static Get convertBytesToGet(byte[] bytes) throws IOException {
        ClientProtos.Get proto = ClientProtos.Get.parseFrom(bytes);
        return ProtobufUtil.toGet(proto);
    }

    public static String convertGetToString(Get get) throws IOException {
        byte[] bytes = convertGetToBytes(get);
        return Base64.encodeBytes(bytes);
    }

    public static Get convertStringToGet(String str) throws IOException {
        byte[] bytes = Base64.decode(str);
        return convertBytesToGet(bytes);
    }

    public static String[] parseTimeColumn(Configuration conf, Log logger) throws IOException {
        String timeColumn = conf.get(TxConstants.OB_ARG_TIME_COLUMN);
        logger.debug("data observer argument " + TxConstants.OB_ARG_TIME_COLUMN +
                " = " + timeColumn);
        if (TxUtils.isEmpty(timeColumn)) {
            throw new IOException("data observer argument " +
                    TxConstants.OB_ARG_TIME_COLUMN + " missed");
        }

        String[] items = timeColumn.split(":", 2);
        if (items.length != 2) {
            throw new IOException(TxConstants.OB_ARG_TIME_COLUMN + "=" +
                    timeColumn + " invalid(family:qualifier)");
        }
        return items;
    }

    public static short parsePhoenixIndexId(Configuration conf, Log logger) throws IOException {
        String indexIdStr = conf.get(TxConstants.OB_ARG_PHOENIX_INDEX_ID);
        logger.debug("data observer argument " + TxConstants.OB_ARG_PHOENIX_INDEX_ID +
                " = " + indexIdStr);
        if (TxUtils.isEmpty(indexIdStr)) {
            throw new IOException("data observer argument " +
                    TxConstants.OB_ARG_PHOENIX_INDEX_ID + " missed");
        }

        short indexId;
        try {
            indexId = Short.parseShort(indexIdStr);
        } catch (NumberFormatException ex) {
            throw new IOException("data observer argument " +
                    TxConstants.OB_ARG_PHOENIX_INDEX_ID + " is invalid", ex);
        }
        return indexId;
    }

    public static TableName getIndexTableName(byte[] dataTableName) {
        return TableName.valueOf(MetaDataUtil.getLocalIndexPhysicalName(dataTableName));
    }

    public static boolean equalCell(Cell cell, byte[] family, byte[] qualifier) {
        return Bytes.equals(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                family, 0, family.length) &&
               Bytes.equals(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                qualifier, 0, qualifier.length);
    }

    public static Get createDataGet() {
        return new Get(TxConstants.TRUE_BYTES);
    }
}
