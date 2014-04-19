package org.rbkluster.hgraph;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
	
	public void addVertex(byte[] vid) throws IOException {
		HTableInterface table = pool.getTable(vtxTable);
		try {
			Put p = new Put(vid);
			p.add(VTX_CF, VTX_IS_Q, TRUE);
			table.put(p);
		} finally {
			table.close();
		}
	}
	
	public void addEdge(byte[] eid, byte[] vout, byte[] vin) throws IOException {
		HTableInterface table = pool.getTable(edgTable);
		try {
			Put p = new Put(eid);
			p.add(EDG_CF, EDG_IS_Q, TRUE);
			p.add(EDG_CF, EDG_OUT_Q, vout);
			p.add(EDG_CF, EDG_IN_Q, vin);
			table.put(p);
		} finally {
			table.close();
		}
		table = pool.getTable(vtxTable);
		try {
			Put p = new Put(Bytes.add(vout, eid));
			p.add(VTX_OUT_CF, vout, vin);
			table.put(p);
			p = new Put(Bytes.add(vin, eid));
			p.add(VTX_IN_CF, vin, vout);
			table.put(p);
		} finally {
			table.close();
		}
	}
	
	public Iterable<byte[][]> getEdgesOut(final byte[] vid) throws IOException {
		return new Iterable<byte[][]>() {
			@Override
			public Iterator<byte[][]> iterator() {
				Scan scan = new Scan(vid);
				scan.setStopRow(GBytes.endKey(vid));
				scan.addFamily(VTX_OUT_CF);
				scan.setBatch(8192);
				scan.setCaching(8192);
				ResultScanner scanner;
				HTableInterface table = pool.getTable(vtxTable);
				try {
					scanner = table.getScanner(scan);
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				final Iterator<Result> sci = scanner.iterator();
				
				return new Iterator<byte[][]>() {
					byte[][] next;
					
					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
					
					@Override
					public byte[][] next() {
						if(!hasNext())
							throw new NoSuchElementException();
						byte[][] n = next;
						next = null;
						return n;
					}
					
					@Override
					public boolean hasNext() {
						while(next == null) {
							if(!sci.hasNext())
								break;
							Result r = sci.next();
							if(r.getValue(VTX_OUT_CF, vid) == null)
								continue;
							byte[] eid = Bytes.tail(r.getRow(), r.getRow().length - vid.length);
							byte[] vin = r.getValue(VTX_OUT_CF, vid);
							next = new byte[][] {vid, eid, vin};
						}
						return next != null;
					}
				};
			}
		};
	}
}
