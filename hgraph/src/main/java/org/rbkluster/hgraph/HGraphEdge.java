package org.rbkluster.hgraph;

import java.io.IOException;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

public class HGraphEdge extends HGraphElement implements Edge {
	public static final String LABEL = HGraphEdge.class.getName() + ".label";
	
	public HGraphEdge(HRawGraph raw, byte[] id) {
		super(raw, id);
	}

	@Override
	public Vertex getVertex(Direction direction)
			throws IllegalArgumentException {
		try {
			switch(direction) {
			case OUT:
				return new HGraphVertex(raw, raw.getOutVertex(id));
			case IN:
				return new HGraphVertex(raw, raw.getInVertex(id));
			default:
				throw ExceptionFactory.bothIsNotSupported();
			}
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getLabel() {
		return getProperty(LABEL);
	}

}
