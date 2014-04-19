package org.rbkluster.hgraph;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class AbstractHGraphTest {
	private static HBaseTestingUtility util;
	protected static Configuration conf;

	@BeforeClass
	public static void beforeClass() throws Exception {
		util = new HBaseTestingUtility();
		conf = HBaseConfiguration.create();
		
		MiniZooKeeperCluster zoo = util.startMiniZKCluster();
		MiniHBaseCluster hbase = util.startMiniHBaseCluster(1, 2);
		
		conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
		conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zoo.getClientPort());
	}

	@AfterClass
	public static void afterClass() throws Exception {
		util.shutdownMiniHBaseCluster();
		util.shutdownMiniZKCluster();
	}
}
