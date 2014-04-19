package org.rbkluster.hgraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
	
	protected Configuration conf;
	protected HTablePool pool;
	
	public HGraph(byte[] prefix, Configuration conf) {
		this.prefix = prefix;
		this.conf = conf;
		pool = new HTablePool(conf, Integer.MAX_VALUE);
		vtxTable = Bytes.add(prefix, VTX_TABLE);
		vtxPropertiesTable = Bytes.add(prefix, VTXP_TABLE);
		edgTable = Bytes.add(prefix, EDG_TABLE);
		edgPropretiesTable = Bytes.add(prefix, EDGP_TABLE);
		idxsTable = Bytes.add(prefix, IDXS_TABLE);
	}
	
	public void createTables() throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			HTableDescriptor d = new HTableDescriptor(vtxTable);
			d.addFamily(new HColumnDescriptor(VTX_CF));
			d.addFamily(new HColumnDescriptor(VTX_OUT_CF));
			d.addFamily(new HColumnDescriptor(VTX_IN_CF));
			admin.createTable(d);
			
			d = new HTableDescriptor(vtxPropertiesTable);
			d.addFamily(new HColumnDescriptor(VTXP_CF));
			admin.createTable(d);
			
			d = new HTableDescriptor(edgTable);
			d.addFamily(new HColumnDescriptor(EDG_CF));
			admin.createTable(d);
			
			d = new HTableDescriptor(edgPropretiesTable);
			d.addFamily(new HColumnDescriptor(EDGP_CF));
			admin.createTable(d);
			
			d = new HTableDescriptor(idxsTable);
			d.addFamily(new HColumnDescriptor(IDXS_CF));
			admin.createTable(d);
		} finally {
			admin.close();
		}
	}
	
	public void dropTables() throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			admin.disableTable(vtxTable);
			admin.deleteTable(vtxTable);
			
			admin.disableTable(vtxPropertiesTable);
			admin.deleteTable(vtxPropertiesTable);
			
			admin.disableTable(edgTable);
			admin.deleteTable(edgTable);
			
			admin.disableTable(edgPropretiesTable);
			admin.deleteTable(edgPropretiesTable);
			
			admin.disableTable(idxsTable);
			admin.deleteTable(idxsTable);
			
			for(HTableDescriptor d : admin.listTables()) {
				if(Bytes.startsWith(d.getName(), Bytes.add(prefix, IDX_TABLE))) {
					admin.disableTable(d.getName());
					admin.deleteTable(d.getName());
				}
			}
		} finally {
			admin.close();
		}
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
	
	public void removeVertex(byte[] vid) throws IOException {
		for(byte[][] e : getEdgesOut(vid))
			removeEdge(e[1], e[0], e[2]);
		for(byte[][] e : getEdgesIn(vid))
			removeEdge(e[1], e[0], e[2]);
		removeVertexProperties(vid);
		HTableInterface table = pool.getTable(vtxTable);
		try {
			Delete d = new Delete(vid);
			table.delete(d);
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
	
	public void removeEdge(byte[] eid) throws IOException {
		HTableInterface table = pool.getTable(edgTable);
		byte[] vout, vin;
		try {
			Get g = new Get(eid);
			g.addFamily(EDG_CF);
			Result r = table.get(g);
			vout = r.getValue(EDG_CF, EDG_OUT_Q);
			vin = r.getValue(EDG_CF, EDG_IN_Q);
		} finally {
			table.close();
		}
		removeEdge(eid, vout, vin);
	}
	
	public void removeEdge(byte[] eid, byte[] vout, byte[] vin) throws IOException {
		HTableInterface table = pool.getTable(edgTable);
		try {
			Delete d = new Delete(eid);
			table.delete(d);
		} finally {
			table.close();
		}
		table = pool.getTable(vtxTable);
		try {
			Delete d = new Delete(Bytes.add(vout, eid));
			d.deleteColumn(VTX_OUT_CF, vout);
			table.delete(d);
			d = new Delete(Bytes.add(vin, eid));
			d.deleteColumn(VTX_IN_CF, vin);
			table.delete(d);
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
				final HTableInterface table = pool.getTable(vtxTable);
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
						if(next == null)
							try {
								table.close();
							} catch(IOException e) {
								throw new RuntimeException(e);
							}
						return next != null;
					}
					
					@Override
					protected void finalize() throws Throwable {
						table.close();
					}
				};
			}
		};
	}

	public Iterable<byte[][]> getEdgesIn(final byte[] vid) throws IOException {
		return new Iterable<byte[][]>() {
			@Override
			public Iterator<byte[][]> iterator() {
				Scan scan = new Scan(vid);
				scan.setStopRow(GBytes.endKey(vid));
				scan.addFamily(VTX_IN_CF);
				scan.setBatch(8192);
				scan.setCaching(8192);
				ResultScanner scanner;
				final HTableInterface table = pool.getTable(vtxTable);
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
							if(r.getValue(VTX_IN_CF, vid) == null)
								continue;
							byte[] eid = Bytes.tail(r.getRow(), r.getRow().length - vid.length);
							byte[] vout = r.getValue(VTX_IN_CF, vid);
							next = new byte[][] {vout, eid, vid};
						}
						if(next == null)
							try {
								table.close();
							} catch(IOException e) {
								throw new RuntimeException(e);
							}
						return next != null;
					}
					
					@Override
					protected void finalize() throws Throwable {
						table.close();
					}
				};
			}
		};
	}
	
	public void setVertexProperty(byte[] vid, byte[] pkey, byte[] pval) throws IOException {
		HTableInterface table = pool.getTable(vtxPropertiesTable);
		try {
			Put p = new Put(vid);
			p.add(VTXP_CF, pkey, pval);
			table.put(p);
		} finally {
			table.close();
		}
	}
	
	public byte[] getVertexProperty(byte[] vid, byte[] pkey) throws IOException {
		HTableInterface table = pool.getTable(vtxPropertiesTable);
		try {
			Get g = new Get(vid);
			g.addColumn(VTXP_CF, pkey);
			Result r = table.get(g);
			return r.getValue(VTXP_CF, pkey);
		} finally {
			table.close();
		}
	}
	
	public void removeVertexProperty(byte[] vid, byte[] pkey) throws IOException {
		HTableInterface table = pool.getTable(vtxPropertiesTable);
		try {
			Delete d = new Delete(vid);
			d.deleteColumn(VTXP_CF, pkey);
			table.delete(d);
		} finally {
			table.close();
		}
	}
	
	public void removeVertexProperties(byte[] vid) throws IOException {
		HTableInterface table = pool.getTable(vtxPropertiesTable);
		try {
			Delete d = new Delete(vid);
			table.delete(d);
		} finally {
			table.close();
		}
	}
	
	public Iterable<byte[][]> getVertexProperties(final byte[] vid) throws IOException {
		return new Iterable<byte[][]>() {
			@Override
			public Iterator<byte[][]> iterator() {
				List<byte[][]> ret = new ArrayList<>();
				try {
					HTableInterface table = pool.getTable(vtxPropertiesTable);
					try {
						Get g = new Get(vid);
						g.addFamily(VTXP_CF);
						Result r = table.get(g);
						for(byte[] pkey : r.getFamilyMap(VTXP_CF).keySet())
							ret.add(new byte[][] {pkey, r.getValue(VTXP_CF, pkey)});
					} finally {
						table.close();
					}
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				return ret.iterator();
			}
		};
	}
}
