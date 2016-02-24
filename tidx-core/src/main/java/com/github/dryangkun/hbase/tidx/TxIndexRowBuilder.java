package com.github.dryangkun.hbase.tidx;

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PUnsignedSmallint;

import java.math.BigDecimal;
import java.util.Arrays;

public class TxIndexRowBuilder {

    private final byte[] prefix;

    public TxIndexRowBuilder(byte[] regionStartKey, byte[] regionEndKey, short phoenixIndexId) {
        byte[] indexIdBytes = PUnsignedSmallint.INSTANCE.toBytes(phoenixIndexId);
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

    public byte[] build(long value, byte[] row) {
        byte[] valueBytes = PDecimal.INSTANCE.toBytes(new BigDecimal(value));
        int rowLength = row != null ? row.length : 0;

        byte[] indexRow = new byte[prefix.length + valueBytes.length + 1 + rowLength];

        int offset = 0;
        System.arraycopy(prefix, 0, indexRow, offset, prefix.length);
        offset += prefix.length;
        System.arraycopy(valueBytes, 0, indexRow, offset, valueBytes.length);
        offset += valueBytes.length;
        indexRow[offset] = QueryConstants.SEPARATOR_BYTE;

        if (rowLength > 0) {
            offset += 1;
            System.arraycopy(row, 0, indexRow, offset, rowLength);
        }

        return indexRow;
    }

    public byte[] build(long value) {
        return build(value, null);
    }
}
