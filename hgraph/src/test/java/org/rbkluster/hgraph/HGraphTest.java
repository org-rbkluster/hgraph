package org.rbkluster.hgraph;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.tinkerpop.blueprints.Vertex;

public class HGraphTest extends AbstractHGraphTest {
	@Test
	public void testProperties() throws Exception {
		HRawGraph raw = new HRawGraph(Bytes.toBytes("proptest"), conf);
		raw.createTables();
		try {
			HGraph g = new HGraph(raw);

			Vertex v = g.addVertex(null);

			v.setProperty("foo", "bar");
			Assert.assertEquals("bar", v.getProperty("foo"));
			
			Assert.assertTrue(g.getVertices().iterator().hasNext());
			Assert.assertTrue(g.getVertices("foo", "bar").iterator().hasNext());
			
		} finally {
			raw.dropTables();
		}
	}
}
