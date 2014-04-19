package org.rbkluster.hgraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
	
	@Test
	public void testIndexes() throws Exception {
		byte[] foo = Bytes.toBytes("foo");
		
		HGraph hg = new HGraph(Bytes.toBytes("test"), conf);
		hg.createTables();
		try {
			hg.createIndex(foo);
			
			byte[] v1 = Bytes.toBytes(1L);
			byte[] v2 = Bytes.toBytes(2L);
			byte[] bar = Bytes.toBytes("bar");
			byte[] qux = Bytes.toBytes("qux");
			
			hg.addVertex(v1);
			hg.addVertex(v2);
			
			hg.setVertexProperty(v1, foo, bar);
			hg.setVertexProperty(v2, foo, qux);
			
			Set<byte[]> bars = new TreeSet<>(Bytes.BYTES_COMPARATOR);
			for(byte[][] iv : hg.getIndexedVertices(foo, bar))
				bars.add(iv[2]);
			
			Set<byte[]> exp = new TreeSet<>(Bytes.BYTES_COMPARATOR);
			exp.add(v1);

			Assert.assertEquals(exp, bars);

			hg.removeVertexProperty(v1, foo);
			
			bars.clear(); exp.clear();
			for(byte[][] iv : hg.getIndexedVertices(foo, bar))
				bars.add(iv[2]);
			
			Assert.assertEquals(exp, bars);
		} finally {
			hg.dropTables();
		}
	}
}
