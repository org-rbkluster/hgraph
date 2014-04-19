package org.rbkluster.hgraph;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

import static org.rbkluster.hgraph.GConstants.*;

public class HGraph {
	protected byte[] prefix;
	protected byte[] vtxTable;
	protected byte[] vtxPropertiesTable;
	protected byte[] edgTable;
	protected byte[] edgPropretiesTable;
	protected byte[] idxsTable;
	protected Map<byte[], byte[]> idxTables = new TreeMap<>(Bytes.BYTES_COMPARATOR);
	
	protected HTablePool pool;
	
	public HGraph(byte[] prefix, HTablePool pool) {
		this.prefix = prefix;
		vtxTable = Bytes.add(prefix, VTX_TABLE);
		vtxPropertiesTable = Bytes.add(prefix, VTXP_TABLE);
		edgTable = Bytes.add(prefix, EDG_TABLE);
		edgPropretiesTable = Bytes.add(prefix, EDGP_TABLE);
		idxsTable = Bytes.add(prefix, IDXS_TABLE);
	}
	
	protected byte[] idxTable(byte[] key) {
		byte[] t = idxTables.get(key);
		if(t != null)
			return t;
		idxTables.put(key, t = Bytes.add(prefix, IDX_TABLE, key));
		return t;
	}
}
