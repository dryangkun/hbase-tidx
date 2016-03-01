package com.github.dryangkun.hbase.tidx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class TxIndexRowCodecTest {

    @Test public void testEncodeNoSaltedTable() throws Exception {
        TxIndexRowCodec indexRowCodec = new TxIndexRowCodec("".getBytes(), "".getBytes(), (short) -32768);
        byte[] a = indexRowCodec.encode(100, "111".getBytes());
        Assert.assertEquals(Bytes.toStringBinary(a), "\\x00\\x00\\xC2\\x02\\x00111");
    }

    @Test public void testEncodeSaltedTable() throws Exception {
        TxIndexRowCodec indexRowCodec = new TxIndexRowCodec("".getBytes(), "1".getBytes(), (short) -32768);
        byte[] a = indexRowCodec.encode(100, "0111".getBytes());
        Assert.assertEquals(Bytes.toStringBinary(a), "\\x00\\x00\\x00\\xC2\\x02\\x000111");
    }

    @Test public void testDecodeTime() throws Exception {
        TxIndexRowCodec indexRowCodec = new TxIndexRowCodec("".getBytes(), "1".getBytes(), (short) -32768);
        byte[] row = Bytes.toBytesBinary("\\x00\\x00\\x00\\xC2\\x02\\x000111");
        long time = indexRowCodec.decodeTime(row, 0, row.length);
        Assert.assertEquals(100, time);
    }

    @Test public void testDecodeDataRow() throws Exception {
        TxIndexRowCodec indexRowCodec = new TxIndexRowCodec("".getBytes(), "1".getBytes(), (short) -32768);
        byte[] row = Bytes.toBytesBinary("\\x00\\x00\\x00\\xC2\\x02\\x000111");
        byte[] dRow = indexRowCodec.decodeDataRow(row, 0, row.length);
        Assert.assertTrue(Bytes.equals(dRow, "0111".getBytes()));
    }
}
