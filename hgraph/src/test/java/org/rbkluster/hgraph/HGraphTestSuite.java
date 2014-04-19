package org.rbkluster.hgraph;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.hbase.util.Bytes;

import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public class HGraphTestSuite extends GraphTest {

	public void testVertexTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new VertexTestSuite(this));
		printTestPerformance("VertexTestSuite", this.stopWatch());
	}

	public void testEdgeTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new EdgeTestSuite(this));
		printTestPerformance("EdgeTestSuite", this.stopWatch());
	}

	public void testGraphTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new GraphTestSuite(this));
		printTestPerformance("GraphTestSuite", this.stopWatch());
	}

	public void testKeyIndexableGraphTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new KeyIndexableGraphTestSuite(this));
		printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
	}

	public Graph generateGraph() {
		return generateGraph(new Object().toString());
	}

	public void doTestSuite(final TestSuite testSuite) throws Exception {
		String doTest = System.getProperty("testTinkerGraph");
		if (doTest == null || doTest.equals("true")) {
			for (Method method : testSuite.getClass().getDeclaredMethods()) {
				if (method.getName().startsWith("test")) {
					System.out.println("Testing " + method.getName() + "...");
					method.invoke(testSuite);
				}
			}
		}
	}

	@Override
	public Graph generateGraph(String graphDirectoryName) {
		graphDirectoryName = graphDirectoryName.replaceAll("[^a-zA-Z_0-9\\-.]", "_");
		HRawGraph raw;
		try {
			raw = new HRawGraph(Bytes.toBytes(graphDirectoryName), AbstractHGraphTest.conf);
			raw.createTables();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return new HGraph(raw) {
			@Override
			public void shutdown() {
				try {
					raw.dropTables();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				super.shutdown();
			}
		};
	}

}
