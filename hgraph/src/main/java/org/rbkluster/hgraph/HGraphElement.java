package org.rbkluster.hgraph;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

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
		Set<String> keys = new TreeSet<>();
		
		Iterable<byte[][]> pki;
		try {
			if(this instanceof Vertex)
				pki = raw.getVertexProperties(id);
			else if(this instanceof Edge)
				pki = raw.getEdgeProperties(id);
			else
				throw new IllegalStateException("neither vertex nor edge:" + this);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
		
		for(byte[][] pk : pki) {
			byte[] k = pk[0];
			if(k.length > TYPE_SUFFIX.length) {
				byte[] tail = Bytes.tail(k, TYPE_SUFFIX.length);
				if(Arrays.equals(tail, TYPE_SUFFIX))
					continue;
			}
			keys.add(Bytes.toString(k));
		}
		
		if(this instanceof Edge)
			keys.remove(HGraphEdge.LABEL);
		
		return keys;
	}

	@Override
	public void setProperty(String key, Object value) {
		if(key.isEmpty() || "id".equals(key))
			throw new IllegalArgumentException();
		if((this instanceof Edge) && StringFactory.LABEL.equals(key))
			throw new IllegalArgumentException();
		forceProperty(key, value);
	}

	protected void forceProperty(String key, Object value) {
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
		return new HGraphId(id);
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
	
	@Override
	public String toString() {
		if(this instanceof Vertex)
			return StringFactory.vertexString((Vertex) this);
		if(this instanceof Edge)
			return StringFactory.edgeString((Edge) this);
		throw new IllegalStateException("neither vertex nor edge:" + this);
	}

}
