package org.rbkluster.hgraph;

import org.junit.Test;

import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;

public class HGraphBlueprintsTest extends AbstractHGraphTest {
	
	@Test
	public void testVertexTestSuite() throws Exception {
		new HGraphBlueprintsTestSuite().testVertexTestSuite();
	}

	@Test
	public void testEdgeTestSuite() throws Exception {
		new HGraphBlueprintsTestSuite().testEdgeTestSuite();
	}

	@Test
	public void testGraphTestSuite() throws Exception {
		new HGraphBlueprintsTestSuite().testGraphTestSuite();
	}

	@Test
	public void testKeyIndexableGraphTestSuite() throws Exception {
		new HGraphBlueprintsTestSuite().testKeyIndexableGraphTestSuite();
	}

}
