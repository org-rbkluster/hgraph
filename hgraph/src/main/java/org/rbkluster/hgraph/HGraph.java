package org.rbkluster.hgraph;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
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
	private static final byte[] META_ROW = Bytes.toBytes(HGraph.class.getName() + ".META_ROW");
	private static final byte[] VERTEX_INDEXES = Bytes.toBytes("vertex_indexes");
	private static final byte[] EDGE_INDEXES = Bytes.toBytes("edge_indexes");

	protected HRawGraph raw;
	protected Set<String> vertexIndexes = new TreeSet<>();
	protected Set<String> edgeIndexes = new TreeSet<>();
	
	public HGraph(HRawGraph raw) throws IOException {
		this.raw = raw;
		raw.addVertex(META_ROW);
		
		if(raw.getVertexProperty(META_ROW, VERTEX_INDEXES) == null)
			raw.setVertexProperty(META_ROW, VERTEX_INDEXES, GBytes.toKryoBytes(vertexIndexes));
		if(raw.getVertexProperty(META_ROW, EDGE_INDEXES) == null)
			raw.setVertexProperty(META_ROW, EDGE_INDEXES, GBytes.toKryoBytes(edgeIndexes));
		
		if(raw.getVertexProperty(META_ROW, VERTEX_INDEXES) != null)
			vertexIndexes = (Set<String>) GBytes.fromKryoBytes(raw.getVertexProperty(META_ROW, VERTEX_INDEXES));
		if(raw.getVertexProperty(META_ROW, EDGE_INDEXES) != null)
			edgeIndexes = (Set<String>) GBytes.fromKryoBytes(raw.getVertexProperty(META_ROW, EDGE_INDEXES));
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
		f.supportsEdgeIteration = true;
		f.supportsEdgeKeyIndex = true;
		f.supportsEdgeProperties = true;
		f.supportsEdgeRetrieval = true;
		f.supportsFloatProperty = true;
		f.supportsIndices = false;
		f.supportsIntegerProperty = true;
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
		f.supportsVertexIteration = true;
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
		if(id == null)
			return null;
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
		return new Iterable<Vertex>() {
			@Override
			public Iterator<Vertex> iterator() {
				final Iterator<byte[]> vids = raw.getAllVertices().iterator();
				return new Iterator<Vertex>() {
					byte[] next;
					
					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
					
					@Override
					public Vertex next() {
						if(!hasNext())
							throw new NoSuchElementException();
						try {
							return new HGraphVertex(raw, next);
						} finally {
							next = null;
						}
					}
					
					@Override
					public boolean hasNext() {
						while(next == null) {
							if(!vids.hasNext())
								break;
							byte[] n = vids.next();
							if(!Arrays.equals(META_ROW, n))
								next = n;
						}
						return next != null;
					}
				};
			}
		};
	}

	@Override
	public Iterable<Vertex> getVertices(final String key, final Object value) {
		if(vertexIndexes.contains(key)) {
			return new Iterable<Vertex>() {
				@Override
				public Iterator<Vertex> iterator() {
					byte[] pkey = Bytes.toBytes(key);
					byte[] pval = GBytes.toKryoBytes(value);
					pval = Bytes.tail(pval, pval.length - 1);
					final Iterator<byte[][]> vids = raw.getIndexedVertices(pkey, pval).iterator();
					return new Iterator<Vertex>() {
						@Override
						public void remove() {
							throw new UnsupportedOperationException();
						}
						
						@Override
						public Vertex next() {
							return new HGraphVertex(raw, vids.next()[2]);
						}
						
						@Override
						public boolean hasNext() {
							return vids.hasNext();
						}
					};
				}
			};
		}
		Predicate<Vertex> pred = new Predicate<Vertex>() {
			@Override
			public boolean apply(Vertex input) {
				return value == null ? input.getProperty(key) == null : value.equals(input.getProperty(key));
			}
		};
		return Iterables.filter(getVertices(), pred);
	}

	@Override
	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
		byte[] eid = (byte[]) id;
		byte[] vout = (byte[]) outVertex.getId();
		byte[] vin = (byte[]) inVertex.getId();
		if(label == null)
			throw new IllegalArgumentException();
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
		if(id == null)
			return null;
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
		return new Iterable<Edge>() {
			@Override
			public Iterator<Edge> iterator() {
				final Iterator<byte[]> eids = raw.getAllEdges().iterator();
				return new Iterator<Edge>() {
					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
					
					@Override
					public Edge next() {
						return new HGraphEdge(raw, eids.next());
					}
					
					@Override
					public boolean hasNext() {
						return eids.hasNext();
					}
				};
			}
		};
	}

	@Override
	public Iterable<Edge> getEdges(final String key, final Object value) {
		if(edgeIndexes.contains(key)) {
			return new Iterable<Edge>() {
				@Override
				public Iterator<Edge> iterator() {
					byte[] pkey = Bytes.toBytes(key);
					byte[] pval = GBytes.toKryoBytes(value);
					pval = Bytes.tail(pval, pval.length - 1);
					final Iterator<byte[][]> eids = raw.getIndexedEdges(pkey, pval).iterator();
					return new Iterator<Edge>() {
						@Override
						public void remove() {
							throw new UnsupportedOperationException();
						}
						
						@Override
						public Edge next() {
							return new HGraphEdge(raw, eids.next()[2]);
						}
						
						@Override
						public boolean hasNext() {
							return eids.hasNext();
						}
					};
				}
			};
		}
		Predicate<Edge> pred = new Predicate<Edge>() {
			@Override
			public boolean apply(Edge input) {
				return value == null ? input.getProperty(key) == null : value.equals(input.getProperty(key));
			}
		};
		return Iterables.filter(getEdges(), pred);
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
		if(elementClass == Vertex.class)
			vertexIndexes.remove(key);
		if(elementClass == Edge.class)
			edgeIndexes.remove(key);
		try {
			raw.setVertexProperty(META_ROW, VERTEX_INDEXES, GBytes.toKryoBytes(vertexIndexes));
			raw.setVertexProperty(META_ROW, EDGE_INDEXES, GBytes.toKryoBytes(edgeIndexes));
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		byte[] pkey = Bytes.toBytes(key);
		if(!vertexIndexes.contains(key) && !edgeIndexes.contains(key) && raw.getIndexKeys().contains(pkey))
			try {
				raw.dropIndex(pkey);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {
		boolean add = false;
		if(elementClass == Vertex.class)
			add = vertexIndexes.add(key);
		if(elementClass == Edge.class)
			add = edgeIndexes.add(key);
		try {
			raw.setVertexProperty(META_ROW, VERTEX_INDEXES, GBytes.toKryoBytes(vertexIndexes));
			raw.setVertexProperty(META_ROW, EDGE_INDEXES, GBytes.toKryoBytes(edgeIndexes));
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		byte[] pkey = Bytes.toBytes(key);
		try {
			if(!raw.getIndexKeys().contains(pkey))
				raw.createIndex(pkey);
			if(add && elementClass == Vertex.class)
				raw.reindexVertices(pkey);
			if(add && elementClass == Edge.class)
				raw.reindexEdges(pkey);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
		if(elementClass == Vertex.class)
			return vertexIndexes;
		if(elementClass == Edge.class)
			return edgeIndexes;
		return null;
	}

}
