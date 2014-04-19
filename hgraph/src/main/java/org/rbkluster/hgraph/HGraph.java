package org.rbkluster.hgraph;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;

public class HGraph implements Graph, KeyIndexableGraph {

	protected HRawGraph raw;
	
	public HGraph(HRawGraph raw) {
		this.raw = raw;
	}
	
	@Override
	public Features getFeatures() {
		Features f = new Features();
		
		f.ignoresSuppliedIds = true;
		f.isPersistent = true;
		f.isWrapper = false;
		f.supportsBooleanProperty = true;
		f.supportsDoubleProperty = true;
		f.supportsDuplicateEdges = true;
		f.supportsEdgeIndex = false;
		f.supportsEdgeIteration = false;
		f.supportsEdgeKeyIndex = true;
		f.supportsEdgeProperties = true;
		f.supportsEdgeRetrieval = true;
		f.supportsFloatProperty = true;
		f.supportsIndices = false;
		f.supportsKeyIndices = true;
		f.supportsLongProperty = true;
		f.supportsMapProperty = true;
		f.supportsMixedListProperty = true;
		f.supportsPrimitiveArrayProperty = true;
		f.supportsSelfLoops = true;
		f.supportsSerializableObjectProperty = true;
		f.supportsStringProperty = true;
		f.supportsThreadedTransactions = false;
		f.supportsTransactions = false;
		f.supportsUniformListProperty = true;
		f.supportsVertexIndex = false;
		f.supportsVertexIteration = false;
		f.supportsVertexKeyIndex = true;
		f.supportsVertexProperties = true;
		
		return f;
	}

	@Override
	public Vertex addVertex(Object id) {
		byte[] vid = (byte[]) id;
		try {
			return new HGraphVertex(raw, raw.addVertex(vid));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Vertex getVertex(Object id) {
		byte[] vid = (byte[]) id;
		try {
			if(!raw.vertexExists(vid))
				return null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return new HGraphVertex(raw, vid);
	}

	@Override
	public void removeVertex(Vertex vertex) {
		byte[] vid = (byte[]) vertex.getId();
		try {
			raw.removeVertex(vid);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Iterable<Vertex> getVertices() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Vertex> getVertices(String key, Object value) {
		final byte[] ikey = Bytes.toBytes(key);
		byte[] kryoValue = GBytes.toKryoBytes(value);
		final byte[] ivalue = Bytes.tail(kryoValue, kryoValue.length - 1);
		if(!raw.getIndexKeys().contains(ikey))
			throw new IllegalArgumentException("No index for property:" + key);
		return new Iterable<Vertex>() {
			@Override
			public Iterator<Vertex> iterator() {
				final Iterator<byte[][]> ii = raw.getIndexedVertices(ikey, ivalue).iterator();
				return new Iterator<Vertex>() {
					
					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
					
					@Override
					public Vertex next() {
						return new HGraphVertex(raw, ii.next()[2]);
					}
					
					@Override
					public boolean hasNext() {
						return ii.hasNext();
					}
				};
			}
		};
	}

	@Override
	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
		byte[] eid = (byte[]) id;
		byte[] vout = (byte[]) outVertex.getId();
		byte[] vin = (byte[]) inVertex.getId();
		Edge e;
		try {
			e = new HGraphEdge(raw, raw.addEdge(eid, vout, vin));
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
		e.setProperty(HGraphEdge.LABEL, label);
		return e;
	}

	@Override
	public Edge getEdge(Object id) {
		byte[] eid = (byte[]) id;
		try {
			if(!raw.edgeExists(eid))
				return null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return new HGraphEdge(raw, eid);
	}

	@Override
	public void removeEdge(Edge edge) {
		byte[] eid = (byte[]) edge.getId();
		try {
			raw.removeEdge(eid);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Iterable<Edge> getEdges() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Edge> getEdges(String key, Object value) {
		final byte[] ikey = Bytes.toBytes(key);
		byte[] kryoValue = GBytes.toKryoBytes(value);
		final byte[] ivalue = Bytes.tail(kryoValue, kryoValue.length - 1);
		if(!raw.getIndexKeys().contains(ikey))
			throw new IllegalArgumentException("No index for property:" + key);
		return new Iterable<Edge>() {
			@Override
			public Iterator<Edge> iterator() {
				final Iterator<byte[][]> ii = raw.getIndexedEdges(ikey, ivalue).iterator();
				return new Iterator<Edge>() {
					
					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
					
					@Override
					public Edge next() {
						return new HGraphEdge(raw, ii.next()[2]);
					}
					
					@Override
					public boolean hasNext() {
						return ii.hasNext();
					}
				};
			}
		};
	}

	@Override
	public GraphQuery query() {
		return new DefaultGraphQuery(this);
	}

	@Override
	public void shutdown() {
		try {
			raw.shutdown();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
		try {
			raw.dropIndex(Bytes.toBytes(key));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {
		try {
			raw.createIndex(Bytes.toBytes(key));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
		Set<String> iks = new TreeSet<>();
		for(byte[] ik : raw.getIndexKeys())
			iks.add(Bytes.toString(ik));
		return iks;
	}

}
