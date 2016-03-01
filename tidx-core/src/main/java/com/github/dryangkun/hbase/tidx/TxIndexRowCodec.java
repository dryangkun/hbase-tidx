package com.github.dryangkun.hbase.tidx;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PUnsignedSmallint;

import java.math.BigDecimal;
import java.util.Arrays;

public class TxIndexRowCodec {

    private final byte[] prefix;

    public TxIndexRowCodec(byte[] regionStartKey, byte[] regionEndKey, short phoenixIndexId) {
        byte[] indexIdBytes = MetaDataUtil.getViewIndexIdDataType().toBytes(phoenixIndexId);
        int regionKeyLength = 0;
        if (regionStartKey != null) {
            regionKeyLength = regionStartKey.length != 0 ? regionStartKey.length : regionEndKey.length;
        }
        prefix = new byte[regionKeyLength + indexIdBytes.length];

        if (regionKeyLength > 0) {
            if (regionStartKey.length != 0) {
                System.arraycopy(regionStartKey, 0, prefix, 0, regionKeyLength);
            } else {
                Arrays.fill(prefix, 0, regionKeyLength, (byte) 0);
            }
        }
        System.arraycopy(indexIdBytes, 0, prefix, prefix.length - indexIdBytes.length, indexIdBytes.length);
    }

    public byte[] encode(long value, byte[] dataRow) {
        byte[] valueBytes = PDecimal.INSTANCE.toBytes(new BigDecimal(value));
        int rowLength = dataRow != null ? dataRow.length : 0;

        byte[] indexRow = new byte[prefix.length + valueBytes.length + 1 + rowLength];

        int offset = 0;
        System.arraycopy(prefix, 0, indexRow, offset, prefix.length);
        offset += prefix.length;
        System.arraycopy(valueBytes, 0, indexRow, offset, valueBytes.length);
        offset += valueBytes.length;
        indexRow[offset] = QueryConstants.SEPARATOR_BYTE;

        if (rowLength > 0) {
            offset += 1;
            System.arraycopy(dataRow, 0, indexRow, offset, rowLength);
        }

        return indexRow;
    }

    public byte[] encode(long value) {
        return encode(value, null);
    }

    private void checkRow(byte[] row, int offset, int length) throws Exception {
        if (length <= prefix.length) {
            throw new Exception("index row length " + length + " le prefix length " + prefix.length);
        }
    }

    private int findZeroByte(byte[] row, int offset, int length) {
        int _offset = -1;
        for (int i = offset + prefix.length; i < offset + length - 1; i++) {
            byte b = row[i + 1];
            if (b == 0) {
                _offset = i + 1;
                break;
            }
        }
        return _offset;
    }

    public Pair<Long, byte[]> decode(byte[] row) throws Exception {
        return decode(row, 0, row.length);
    }

    public Pair<Long, byte[]> decode(byte[] row, int offset, int length) throws Exception {
        checkRow(row, offset, length);

        int _offset = findZeroByte(row, offset, length);
        if (_offset == -1 || _offset == offset + length - 1) {
            throw new Exception("not found zero byte in index row " + Bytes.toStringBinary(row));
        }
        int _length;

        long time;
        {
            _length = _offset - offset - prefix.length;
            BigDecimal value = (BigDecimal) PDecimal.INSTANCE.toObject(
                    row, offset + prefix.length, _length,
                    PDecimal.INSTANCE, SortOrder.ASC);
            time = value.longValue();
        }

        byte[] dRow;
        {
            _offset += 1;
            _length = offset + length - _offset;
            dRow = new byte[_length];
            System.arraycopy(row, _offset, dRow, 0, _length);
        }

        return new Pair<Long, byte[]>(time, dRow);
    }

    public long decodeTime(byte[] row) throws Exception {
        return decodeTime(row, 0, row.length);
    }

    public long decodeTime(byte[] row, int offset, int length) throws Exception {
        checkRow(row, offset, length);

        int _offset = findZeroByte(row, offset, length);
        if (_offset == -1) {
            throw new Exception("not found zero byte in index row " + Bytes.toStringBinary(row));
        }
        int _length = _offset - offset - prefix.length;

        BigDecimal value = (BigDecimal) PDecimal.INSTANCE.toObject(
                row, offset + prefix.length, _length,
                PDecimal.INSTANCE, SortOrder.ASC);
        return value.longValue();
    }

    public byte[] decodeDataRow(byte[] row) throws Exception {
        return decodeDataRow(row, 0, row.length);
    }

    public byte[] decodeDataRow(byte[] row, int offset, int length) throws Exception {
        checkRow(row, offset, length);

        int _offset = findZeroByte(row, offset, length);
        if (_offset == -1 || _offset == offset + length - 1) {
            throw new Exception("not found zero byte in index row " + Bytes.toStringBinary(row));
        }

        _offset += 1;
        int _length = offset + length - _offset;

        byte[] dRow = new byte[_length];
        System.arraycopy(row, _offset, dRow, 0, _length);
        return dRow;
    }
}
