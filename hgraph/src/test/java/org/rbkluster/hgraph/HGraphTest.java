package org.rbkluster.hgraph;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class HGraphTest extends AbstractHGraphTest {
	
	
	@Test
	public void testEdgesInOut() throws Exception {
		HGraph hg = new HGraph(Bytes.toBytes("test"), conf);
		hg.createTables();
		try {
			byte[] vout = Bytes.toBytes(1L);
			byte[] vin = Bytes.toBytes(2L);
			byte[] eid = Bytes.toBytes(3L);
			
			hg.addVertex(vout);
			hg.addVertex(vin);
			hg.addEdge(eid, vout, vin);
			
			boolean found = false;
			for(byte[][] e : hg.getEdgesOut(vout)) {
				if(Arrays.equals(eid, e[1]))
					found = true;
			}
			Assert.assertTrue("edge out not found on vout", found);
			
			found = false;
			for(byte[][] e : hg.getEdgesIn(vout)) {
				found = true;
			}
			Assert.assertFalse("edge in found on vout", found);
			
			found = false;
			for(byte[][] e : hg.getEdgesIn(vin)) {
				if(Arrays.equals(eid, e[1]))
					found = true;
			}
			Assert.assertTrue("edge in not found on vin", found);
			
			found = false;
			for(byte[][] e : hg.getEdgesOut(vin)) {
				found = true;
			}
			Assert.assertFalse("edge out found on vin", found);
		} finally {
			hg.dropTables();
		}
	}
}
