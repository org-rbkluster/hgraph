package org.rbkluster.hgraph;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.hbase.util.Bytes;

import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public class HGraphBlueprintsTestSuite extends GraphTest {

	protected String methodName;
	
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
		return generateGraph(methodName);
	}

	public void doTestSuite(final TestSuite testSuite) throws Exception {
		String doTest = System.getProperty("testTinkerGraph");
		if (doTest == null || doTest.equals("true")) {
			Method[] dm = testSuite.getClass().getDeclaredMethods();
			Comparator<Method> NAME_ORDER = new Comparator<Method>() {
				@Override
				public int compare(Method o1, Method o2) {
					return o1.getName().compareToIgnoreCase(o2.getName());
				}
			};
			Collections.sort(Arrays.asList(dm), NAME_ORDER);
			for (Method method : dm) {
				if (method.getName().startsWith("test")) {
					methodName = method.getName();
					
					HRawGraph raw;
					try {
						raw = new HRawGraph(Bytes.toBytes(methodName), AbstractHGraphTest.conf);
						raw.dropTables();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
					try {
						System.out.println("Testing " + method.getName() + "...");
						method.invoke(testSuite);
					} finally {
						methodName = null;

						try {
							raw.dropTables();
						} catch(IOException e) {
							throw new RuntimeException(e);
						}
					}
				}
			}
		}
	}

	@Override
	public Graph generateGraph(String graphDirectoryName) {
		if(graphDirectoryName == null)
			graphDirectoryName = new Object().toString();
		graphDirectoryName = graphDirectoryName.replaceAll("[^a-zA-Z_0-9\\-.]", "_");
		HRawGraph raw;
		try {
			raw = new HRawGraph(Bytes.toBytes(graphDirectoryName), AbstractHGraphTest.conf);
			raw.createTables();
			return new HGraph(raw);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
