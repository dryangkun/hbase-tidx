package com.github.dryangkun.hbase.tidx.mapreduce;

import com.github.dryangkun.hbase.tidx.TxConstants;
import com.github.dryangkun.hbase.tidx.TxUtils;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

public class TxJobExample {

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        conf.set(TxConstants.MR_CONF_START_TIME, "100");
        conf.set(TxConstants.MR_CONF_END_TIME, "104");
        conf.set(TxConstants.MR_CONF_PHOENIX_INDEX_ID, "0");
        conf.set(TxConstants.MR_CONF_TIME_CHECK, "true");
        conf.set(TxConstants.MR_CONF_TABLE, TxUtils.getIndexTableName("T1").getNameAsString());

        Get dataGet = TxUtils.createDataGet();
        dataGet.addColumn("0".getBytes(), "A".getBytes());
        dataGet.addColumn("0".getBytes(), "T".getBytes());
        conf.set(TxConstants.MR_CONF_DATA_GET, TxUtils.convertGetToString(dataGet));

        Job job = Job.getInstance(conf, "tx-job-example");

        job.setInputFormatClass(TxIndexTableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setMapperClass(MyMapper.class);
        job.setNumReduceTasks(0);
        job.setJarByClass(TxJobExample.class);
        TableMapReduceUtil.addDependencyJars(job);

        job.waitForCompletion(true);
    }

    public static class MyMapper extends Mapper<ImmutableBytesWritable, Result, NullWritable, NullWritable> {

        private final static Log LOG = LogFactory.getLog(MyMapper.class);

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            LOG.info("result - " + value.toString());
        }
    }
}
