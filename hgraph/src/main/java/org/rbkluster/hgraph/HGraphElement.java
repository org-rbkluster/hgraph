package org.rbkluster.hgraph;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

public abstract class HGraphElement implements Element {
	public static final byte[] TYPE_SUFFIX = Bytes.toBytes("_type");
	
	protected HRawGraph raw;
	protected byte[] id;
	
	protected HGraphElement(HRawGraph raw, byte[] id) {
		this.raw = raw;
		this.id = id;
	}
	
	protected byte[] getRawProperty(byte[] key) {
		try {
			if(this instanceof Vertex)
				return raw.getVertexProperty(id, key);
			if(this instanceof Edge)
				return raw.getEdgeProperty(id, key);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		throw new IllegalStateException("neither vertex nor edge:" + this);
	}
	
	protected void setRawProperty(byte[] key, byte[] val) {
		try {
			if(this instanceof Vertex)
				raw.setVertexProperty(id, key, val);
			else if(this instanceof Edge)
				raw.setEdgeProperty(id, key, val);
			else
				throw new IllegalStateException("neither vertex nor edge:" + this);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected void removeRawProperty(byte[] key) {
		try {
			if(this instanceof Vertex)
				raw.removeVertexProperty(id, key);
			else if(this instanceof Edge)
				raw.removeEdgeProperty(id, key);
			else
				throw new IllegalStateException("neither vertex nor edge:" + this);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getProperty(String key) {
		byte[] keyBytes = Bytes.toBytes(key);
		byte[] typeKeyBytes = Bytes.add(keyBytes, TYPE_SUFFIX);
		byte[] keyValue = getRawProperty(keyBytes);
		byte[] typeValue = getRawProperty(typeKeyBytes);
		if(keyValue == null || typeValue == null)
			return null;
		return (T) GBytes.fromKryoBytes(Bytes.add(typeValue, keyValue));
	}

	@Override
	public Set<String> getPropertyKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setProperty(String key, Object value) {
		byte[] keyBytes = Bytes.toBytes(key);
		byte[] typeKeyBytes = Bytes.add(keyBytes, TYPE_SUFFIX);
		byte[] kryoValue = GBytes.toKryoBytes(value);
		byte[] keyValue = Bytes.tail(kryoValue, kryoValue.length - 1);
		byte[] typeValue = Bytes.head(kryoValue, 1);
		setRawProperty(keyBytes, keyValue);
		setRawProperty(typeKeyBytes, typeValue);
	}

	@Override
	public <T> T removeProperty(String key) {
		T val = getProperty(key);
		byte[] keyBytes = Bytes.toBytes(key);
		byte[] typeKeyBytes = Bytes.add(keyBytes, TYPE_SUFFIX);
		removeRawProperty(keyBytes);
		removeRawProperty(typeKeyBytes);
		return val;
	}

	@Override
	public void remove() {
		try {
			if(this instanceof Vertex)
				raw.removeVertex(id);
			else if(this instanceof Edge)
				raw.removeEdge(id);
			else
				throw new IllegalStateException("neither vertex nor edge:" + this);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object getId() {
		return id;
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(id);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == null)
			return false;
		if(obj == this)
			return true;
		if(getClass() == obj.getClass()) {
			HGraphElement o = (HGraphElement) obj;
			return Arrays.equals(id, o.id);
		}
		return false;
	}

}
