package com.github.dryangkun.hbase.tindex;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PUnsignedSmallint;
import org.apache.phoenix.util.MetaDataUtil;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class TxIndexRowBuilderTest {

    public static void main(String[] args) throws Exception {
        new TxIndexRowBuilderTest().testBuildNoSaltedTable();
    }

    @Test public void testBuildNoSaltedTable() throws Exception {
        TxIndexRowBuilder indexRowBuilder = new TxIndexRowBuilder("".getBytes(), "".getBytes(), (short) 0);
        byte[] a = indexRowBuilder.build(100, "111".getBytes());
        Assert.assertEquals(Bytes.toStringBinary(a), "\\x00\\x00\\xC2\\x02\\x00111");
    }

    @Test public void testBuildSaltedTable() throws Exception {
        TxIndexRowBuilder indexRowBuilder = new TxIndexRowBuilder("".getBytes(), "1".getBytes(), (short) 0);
        byte[] a = indexRowBuilder.build(100, "0111".getBytes());
        Assert.assertEquals(Bytes.toStringBinary(a), "\\x00\\x00\\x00\\xC2\\x02\\x000111");
    }
}
