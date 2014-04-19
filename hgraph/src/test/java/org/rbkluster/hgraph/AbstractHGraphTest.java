package org.rbkluster.hgraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class AbstractHGraphTest {
	private static HBaseTestingUtility util;
	public static Configuration conf;

	@BeforeClass
	public static void beforeClass() throws Exception {
		DOMConfigurator.configure(AbstractHGraphTest.class.getResource("log4j.xml"));
		
		util = new HBaseTestingUtility();
		conf = HBaseConfiguration.create();
		
		MiniZooKeeperCluster zoo = util.startMiniZKCluster();
		util.startMiniHBaseCluster(1, 2);
		
		conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
		conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zoo.getClientPort());
	}

	@AfterClass
	public static void afterClass() throws Exception {
		util.shutdownMiniHBaseCluster();
		util.shutdownMiniZKCluster();
	}
}
