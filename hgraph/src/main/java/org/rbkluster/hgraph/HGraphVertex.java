package org.rbkluster.hgraph;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

public class HGraphVertex extends HGraphElement implements Vertex {

	protected class EdgesIterable implements Iterable<Edge> {
		private final Iterable<byte[][]> eids;

		protected EdgesIterable(Iterable<byte[][]> eids) {
			this.eids = eids;
		}

		@Override
		public Iterator<Edge> iterator() {
			final Iterator<byte[][]> i = eids.iterator();
			return new Iterator<Edge>() {
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
				
				@Override
				public Edge next() {
					return new HGraphEdge(raw, i.next()[1]);
				}
				
				@Override
				public boolean hasNext() {
					return i.hasNext();
				}
			};
		}
	}

	protected class OutVertexIterable implements Iterable<Vertex> {
		private final Iterable<byte[][]> eids;

		protected OutVertexIterable(Iterable<byte[][]> eids) {
			this.eids = eids;
		}

		@Override
		public Iterator<Vertex> iterator() {
			final Iterator<byte[][]> i = eids.iterator();
			return new Iterator<Vertex>() {
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
				
				@Override
				public Vertex next() {
					return new HGraphVertex(raw, i.next()[0]);
				}
				
				@Override
				public boolean hasNext() {
					return i.hasNext();
				}
			};
		}
	}

	protected class InVertexIterable implements Iterable<Vertex> {
		private final Iterable<byte[][]> eids;

		protected InVertexIterable(Iterable<byte[][]> eids) {
			this.eids = eids;
		}

		@Override
		public Iterator<Vertex> iterator() {
			final Iterator<byte[][]> i = eids.iterator();
			return new Iterator<Vertex>() {
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
				
				@Override
				public Vertex next() {
					return new HGraphVertex(raw, i.next()[2]);
				}
				
				@Override
				public boolean hasNext() {
					return i.hasNext();
				}
			};
		}
	}

	
	public HGraphVertex(HRawGraph raw, byte[] id) {
		super(raw, id);
	}

	@Override
	public Iterable<Edge> getEdges(Direction direction, String... labels) {
		Iterable<byte[][]> out = null;
		Iterable<byte[][]> in = null;
		
		try {
			if(direction != Direction.IN)
				out = raw.getEdgesOut(id);
			if(direction != Direction.OUT)
				in = raw.getEdgesIn(id);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		
		Iterable<Edge> eout = null;
		Iterable<Edge> ein = null;
		
		if(out != null)
			eout = new EdgesIterable(out);
		if(in != null)
			ein = new EdgesIterable(in);
		
		if(eout == null)
			return ein;
		if(ein == null)
			return eout;
		return Iterables.concat(eout, ein);
		
	}

	@Override
	public Iterable<Vertex> getVertices(Direction direction, String... labels) {
		Iterable<byte[][]> out = null;
		Iterable<byte[][]> in = null;
		
		try {
			if(direction != Direction.IN)
				out = raw.getEdgesOut(id);
			if(direction != Direction.OUT)
				in = raw.getEdgesIn(id);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		
		Iterable<Vertex> vout = null;
		Iterable<Vertex> vin = null;
		
		if(out != null)
			vout = new OutVertexIterable(out);
		if(in != null)
			vin = new InVertexIterable(in);
		
		if(vout == null)
			return vin;
		if(vin == null)
			return vout;
		return Iterables.concat(vout, vin);
	}

	@Override
	public VertexQuery query() {
		return new DefaultVertexQuery(this);
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex) {
		byte[] vin = (byte[]) inVertex.getId();
		byte[] eid;
		try {
			eid = raw.addEdge(null, id, vin);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		HGraphEdge e = new HGraphEdge(raw, eid);
		e.setProperty(HGraphEdge.LABEL, label);
		return e;
	}

}
