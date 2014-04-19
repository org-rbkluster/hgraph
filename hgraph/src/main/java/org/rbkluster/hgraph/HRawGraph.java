package org.rbkluster.hgraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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

public class HRawGraph {
	protected byte[] prefix;
	protected byte[] vtxTable;
	protected byte[] vtxPropertiesTable;
	protected byte[] edgTable;
	protected byte[] edgPropertiesTable;
	protected Map<byte[], byte[]> idxTables = new TreeMap<>(Bytes.BYTES_COMPARATOR);
	
	protected Configuration conf;
	protected HTablePool pool;
	
	public HRawGraph(byte[] prefix, Configuration conf) throws IOException {
		this.prefix = prefix;
		this.conf = conf;
		pool = new HTablePool(conf, Integer.MAX_VALUE);
		vtxTable = Bytes.add(prefix, VTX_TABLE);
		vtxPropertiesTable = Bytes.add(prefix, VTXP_TABLE);
		edgTable = Bytes.add(prefix, EDG_TABLE);
		edgPropertiesTable = Bytes.add(prefix, EDGP_TABLE);
		
		loadIndexTables();
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
			
			d = new HTableDescriptor(edgPropertiesTable);
			d.addFamily(new HColumnDescriptor(EDGP_CF));
			admin.createTable(d);
		} finally {
			admin.close();
		}
	}
	
	public void loadIndexTables() throws IOException {
		idxTables.clear();
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			for(HTableDescriptor d : admin.listTables()) {
				byte[] p = Bytes.add(prefix, IDX_TABLE);
				if(Bytes.startsWith(d.getName(), p)) {
					byte[] k = Bytes.tail(d.getName(), d.getName().length - p.length);
					idxTables.put(k, d.getName());
				}
			}
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
			
			admin.disableTable(edgPropertiesTable);
			admin.deleteTable(edgPropertiesTable);
			
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
		removeEdgeProperties(eid);
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
		
		if(idxTables.containsKey(pkey)) {
			table = pool.getTable(idxTables.get(pkey));
			try {
				Put p = new Put(Bytes.add(pval, vid));
				p.add(IDX_VTX_CF, pval, vid);
				table.put(p);
			} finally {
				table.close();
			}
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
		byte[] pval = null;
		if(idxTables.containsKey(pkey))
			pval = getVertexProperty(vid, pkey);
		
		HTableInterface table = pool.getTable(vtxPropertiesTable);
		try {
			Delete d = new Delete(vid);
			d.deleteColumn(VTXP_CF, pkey);
			table.delete(d);
		} finally {
			table.close();
		}
		
		if(idxTables.containsKey(pkey)) {
			table = pool.getTable(idxTables.get(pkey));
			try {
				Delete d = new Delete(Bytes.add(pval, vid));
				d.deleteColumn(IDX_VTX_CF, pval);
				table.delete(d);
			} finally {
				table.close();
			}
		}
	}
	
	public void removeVertexProperty(byte[] vid, byte[] pkey, byte[] pval) throws IOException {
		HTableInterface table = pool.getTable(vtxPropertiesTable);
		try {
			Delete d = new Delete(vid);
			d.deleteColumn(VTXP_CF, pkey);
			table.delete(d);
		} finally {
			table.close();
		}
		
		if(idxTables.containsKey(pkey)) {
			table = pool.getTable(idxTables.get(pkey));
			try {
				Delete d = new Delete(Bytes.add(pval, vid));
				d.deleteColumn(IDX_VTX_CF, pval);
				table.delete(d);
			} finally {
				table.close();
			}
		}
	}
	
	public void removeVertexProperties(byte[] vid) throws IOException {
		for(byte[][] p : getVertexProperties(vid))
			removeVertexProperty(vid, p[0], p[1]);
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

	public void setEdgeProperty(byte[] eid, byte[] pkey, byte[] pval) throws IOException {
		HTableInterface table = pool.getTable(edgPropertiesTable);
		try {
			Put p = new Put(eid);
			p.add(EDGP_CF, pkey, pval);
			table.put(p);
		} finally {
			table.close();
		}

		if(idxTables.containsKey(pkey)) {
			table = pool.getTable(idxTables.get(pkey));
			try {
				Put p = new Put(Bytes.add(pval, eid));
				p.add(IDX_EDG_CF, pval, eid);
				table.put(p);
			} finally {
				table.close();
			}
		}
	}
	
	public byte[] getEdgeProperty(byte[] eid, byte[] pkey) throws IOException {
		HTableInterface table = pool.getTable(edgPropertiesTable);
		try {
			Get g = new Get(eid);
			g.addColumn(EDGP_CF, pkey);
			Result r = table.get(g);
			return r.getValue(EDGP_CF, pkey);
		} finally {
			table.close();
		}
	}
	
	public void removeEdgeProperty(byte[] eid, byte[] pkey) throws IOException {
		byte[] pval = null;
		if(idxTables.containsKey(pkey))
			pval = getEdgeProperty(eid, pkey);
		
		HTableInterface table = pool.getTable(edgPropertiesTable);
		try {
			Delete d = new Delete(eid);
			d.deleteColumn(EDGP_CF, pkey);
			table.delete(d);
		} finally {
			table.close();
		}

		if(idxTables.containsKey(pkey)) {
			table = pool.getTable(idxTables.get(pkey));
			try {
				Delete d = new Delete(Bytes.add(pval, eid));
				d.deleteColumn(IDX_EDG_CF, pval);
				table.delete(d);
			} finally {
				table.close();
			}
		}
	}
	
	public void removeEdgeProperty(byte[] eid, byte[] pkey, byte[] pval) throws IOException {
		HTableInterface table = pool.getTable(edgPropertiesTable);
		try {
			Delete d = new Delete(eid);
			d.deleteColumn(EDGP_CF, pkey);
			table.delete(d);
		} finally {
			table.close();
		}

		if(idxTables.containsKey(pkey)) {
			table = pool.getTable(idxTables.get(pkey));
			try {
				Delete d = new Delete(Bytes.add(pval, eid));
				d.deleteColumn(IDX_EDG_CF, pval);
				table.delete(d);
			} finally {
				table.close();
			}
		}
	}
	
	public void removeEdgeProperties(byte[] eid) throws IOException {
		for(byte[][] p : getEdgeProperties(eid))
			removeEdgeProperty(eid, p[0], p[1]);
		HTableInterface table = pool.getTable(edgPropertiesTable);
		try {
			Delete d = new Delete(eid);
			table.delete(d);
		} finally {
			table.close();
		}
	}
	
	public Iterable<byte[][]> getEdgeProperties(final byte[] eid) throws IOException {
		return new Iterable<byte[][]>() {
			@Override
			public Iterator<byte[][]> iterator() {
				List<byte[][]> ret = new ArrayList<>();
				try {
					HTableInterface table = pool.getTable(edgPropertiesTable);
					try {
						Get g = new Get(eid);
						g.addFamily(EDGP_CF);
						Result r = table.get(g);
						for(byte[] pkey : r.getFamilyMap(EDGP_CF).keySet())
							ret.add(new byte[][] {pkey, r.getValue(EDGP_CF, pkey)});
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

	public void createIndex(byte[] pkey) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			HTableDescriptor d = new HTableDescriptor(Bytes.add(prefix, IDX_TABLE, pkey));
			d.addFamily(new HColumnDescriptor(IDX_VTX_CF));
			d.addFamily(new HColumnDescriptor(IDX_EDG_CF));
			admin.createTable(d);
			
			idxTables.put(pkey, d.getName());
		} finally {
			admin.close();
		}
	}
	
	public void dropIndex(byte[] pkey) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			admin.disableTable(idxTables.get(pkey));
			admin.deleteTable(idxTables.get(pkey));
			idxTables.remove(pkey);
		} finally {
			admin.close();
		}
	}
	
	public Iterable<byte[][]> getIndexedVertices(final byte[] pkey, final byte[] pval) {
		return new Iterable<byte[][]>() {
			@Override
			public Iterator<byte[][]> iterator() {
				Scan scan = new Scan(pval);
				scan.setStopRow(GBytes.endKey(pval));
				scan.addFamily(IDX_VTX_CF);
				scan.setBatch(8192);
				scan.setCaching(8192);
				ResultScanner scanner;
				final HTableInterface table = pool.getTable(idxTables.get(pkey));
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
							if(r.getValue(IDX_VTX_CF, pval) == null)
								continue;
							byte[] vid = r.getValue(IDX_VTX_CF, pval);
							next = new byte[][]	 {pkey, pval, vid};
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
	
	public Iterable<byte[][]> getIndexedVertices(final byte[] pkey, final byte[] pvalStart, final byte[] pvalStop) {
		return new Iterable<byte[][]>() {
			@Override
			public Iterator<byte[][]> iterator() {
				Scan scan = new Scan(pvalStart);
				scan.setStopRow(GBytes.endKey(pvalStop));
				scan.addFamily(IDX_VTX_CF);
				scan.setBatch(8192);
				scan.setCaching(8192);
				ResultScanner scanner;
				final HTableInterface table = pool.getTable(idxTables.get(pkey));
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
							if(r.getFamilyMap(IDX_VTX_CF) == null)
								continue;
							for(byte[] pval : r.getFamilyMap(IDX_VTX_CF).keySet()) {
								Comparator<byte[]> c = Bytes.BYTES_COMPARATOR;
								if(c.compare(pvalStart, pval) <= 0 && c.compare(pval, pvalStop) < 0) {
									byte[] vid = r.getValue(IDX_VTX_CF, pval);
									next = new byte[][]	 {pkey, pval, vid};
									break;
								}
							}
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

	public Iterable<byte[][]> getIndexedEdges(final byte[] pkey, final byte[] pval) {
		return new Iterable<byte[][]>() {
			@Override
			public Iterator<byte[][]> iterator() {
				Scan scan = new Scan(pval);
				scan.setStopRow(GBytes.endKey(pval));
				scan.addFamily(IDX_EDG_CF);
				scan.setBatch(8192);
				scan.setCaching(8192);
				ResultScanner scanner;
				final HTableInterface table = pool.getTable(idxTables.get(pkey));
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
							if(r.getValue(IDX_EDG_CF, pval) == null)
								continue;
							byte[] vid = r.getValue(IDX_EDG_CF, pval);
							next = new byte[][]	 {pkey, pval, vid};
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

	public Iterable<byte[][]> getIndexedEdges(final byte[] pkey, final byte[] pvalStart, final byte[] pvalStop) {
		return new Iterable<byte[][]>() {
			@Override
			public Iterator<byte[][]> iterator() {
				Scan scan = new Scan(pvalStart);
				scan.setStopRow(GBytes.endKey(pvalStop));
				scan.addFamily(IDX_EDG_CF);
				scan.setBatch(8192);
				scan.setCaching(8192);
				ResultScanner scanner;
				final HTableInterface table = pool.getTable(idxTables.get(pkey));
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
							if(r.getFamilyMap(IDX_EDG_CF) == null)
								continue;
							for(byte[] pval : r.getFamilyMap(IDX_EDG_CF).keySet()) {
								Comparator<byte[]> c = Bytes.BYTES_COMPARATOR;
								if(c.compare(pvalStart, pval) <= 0 && c.compare(pval, pvalStop) < 0) {
									byte[] eid = r.getValue(IDX_EDG_CF, pval);
									next = new byte[][]	 {pkey, pval, eid};
									break;
								}
							}
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
}
