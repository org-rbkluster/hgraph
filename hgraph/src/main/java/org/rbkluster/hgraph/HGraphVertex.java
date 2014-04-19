package org.rbkluster.hgraph;

import java.io.IOException;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

public class HGraphVertex extends HGraphElement implements Vertex {

	public HGraphVertex(HRawGraph raw, byte[] id) {
		super(raw, id);
	}

	@Override
	public Iterable<Edge> getEdges(Direction direction, String... labels) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<Vertex> getVertices(Direction direction, String... labels) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VertexQuery query() {
		// TODO Auto-generated method stub
		return null;
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
