package org.rbkluster.hgraph;

import java.io.File;

import org.apache.commons.io.FileUtils;
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
		
		System.setProperty("java.io.tmpdir", "target/hbase/tmp");
		System.setProperty("user.home", "target/hbase/home");
		System.setProperty(HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY, "target/hbase/data");
		
		FileUtils.deleteDirectory(new File("target/hbase"));
		
		util = new HBaseTestingUtility();
		conf = HBaseConfiguration.create();
		
		MiniZooKeeperCluster zoo = util.startMiniZKCluster();
		util.startMiniHBaseCluster(1, 1);
		
		conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
		conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zoo.getClientPort());
	}

	@AfterClass
	public static void afterClass() throws Exception {
		util.shutdownMiniHBaseCluster();
		util.shutdownMiniZKCluster();
	}
}
