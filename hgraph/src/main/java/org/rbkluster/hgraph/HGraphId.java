package org.rbkluster.hgraph;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

public class HGraphId {
	protected byte[] id;
	
	public HGraphId(byte[] id) {
		this.id = id;
	}
	
	public HGraphId(String id) {
		this(Bytes.toBytesBinary(id));
	}
	
	public byte[] getId() {
		return id;
	}
	
	@Override
	public String toString() {
		return Bytes.toStringBinary(id);
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
		if(obj instanceof HGraphId) {
			return Arrays.equals(id, ((HGraphId) obj).id);
		}
		return false;
	}
	
}
