package org.rbkluster.hgraph;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.rbkluster.hgraph.GConstants.*;

public class HRawGraph {
	private static final Logger log = LoggerFactory.getLogger(HRawGraph.class);
	
	public static final int DEFAULT_ID_LENGTH = 24;
	
	protected byte[] prefix;
	protected byte[] vtxTable;
	protected byte[] vtxPropertiesTable;
	protected byte[] edgTable;
	protected byte[] edgPropertiesTable;
	protected Map<byte[], byte[]> idxTables = new TreeMap<>(Bytes.BYTES_COMPARATOR);
	
	protected Configuration conf;
	protected HTablePool _pool;
	
	protected SecureRandom random = new SecureRandom();
	
	public HRawGraph(byte[] prefix, Configuration conf) throws IOException {
		this.prefix = tableEscape(prefix);
		this.conf = conf;

		log.info("{} creating graph", this);
		
		_pool = new HTablePool(conf, Integer.MAX_VALUE);
		vtxTable = Bytes.add(this.prefix, VTX_TABLE);
		vtxPropertiesTable = Bytes.add(this.prefix, VTXP_TABLE);
		edgTable = Bytes.add(this.prefix, EDG_TABLE);
		edgPropertiesTable = Bytes.add(this.prefix, EDGP_TABLE);
		
		loadIndexTables();
	}
	
	protected HTableInterface table(byte[] tableName) {
		HTableInterface table = _pool.getTable(tableName);
//		table.setAutoFlush(true);
		return table;
	}
	
	public static byte[] tableEscape(byte[] k) {
		StringBuilder sb = new StringBuilder();
		for(byte b : k) {
			char c = (char) b;
			if(
					c >= 'a' && c <= 'z'
					|| c >= 'A' && c <= 'Z'
					|| c >= '0' && c <= '9'
					|| c == '_'
					|| c == '-'
					|| c == '.')
				sb.append(c);
			else
				sb.append(String.format("_%02x", 0xff & (int) b));
		}
		return Bytes.toBytes(sb.toString());
	}
	
	public static byte[] tableUnescape(byte[] k) {
		String hex = Bytes.toString(k);
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		for(int i = 0; i < hex.length(); i++) {
			if(hex.charAt(i) != '_')
				bout.write(hex.charAt(i));
			else {
				bout.write(Integer.parseInt(hex.substring(i+1, i+3), 16));
				i += 2;
			}
		}
		return bout.toByteArray();
	}
	
	protected void repool(HTableInterface table) throws IOException {
		_pool.putTable(table);
	}
	
	public void createTables() throws IOException {
		log.info("{} creating tables", this);
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			HTableDescriptor d = new HTableDescriptor(vtxTable);
			d.addFamily(new HColumnDescriptor(VTX_CF));
			d.addFamily(new HColumnDescriptor(VTX_OUT_CF));
			d.addFamily(new HColumnDescriptor(VTX_IN_CF));
			if(!admin.tableExists(d.getName())) {
				log.debug("{} creating table {}", this, d.getNameAsString());
				admin.createTable(d);
			} else
				log.debug("{} table {} already exists", this, d.getNameAsString());
			
			d = new HTableDescriptor(vtxPropertiesTable);
			d.addFamily(new HColumnDescriptor(VTXP_CF));
			if(!admin.tableExists(d.getName())) {
				log.debug("{} creating table {}", this, d.getNameAsString());
				admin.createTable(d);
			} else
				log.debug("{} table {} already exists", this, d.getNameAsString());
			
			d = new HTableDescriptor(edgTable);
			d.addFamily(new HColumnDescriptor(EDG_CF));
			if(!admin.tableExists(d.getName())) {
				log.debug("{} creating table {}", this, d.getNameAsString());
				admin.createTable(d);
			} else
				log.debug("{} table {} already exists", this, d.getNameAsString());
			
			d = new HTableDescriptor(edgPropertiesTable);
			d.addFamily(new HColumnDescriptor(EDGP_CF));
			if(!admin.tableExists(d.getName())) {
				log.debug("{} creating table {}", this, d.getNameAsString());
				admin.createTable(d);
			} else
				log.debug("{} table {} already exists", this, d.getNameAsString());
		} finally {
			admin.close();
		}
		log.debug("{} tables created", this);
	}
	
	public void loadIndexTables() throws IOException {
		log.debug("{} loading index tables", this);
		idxTables.clear();
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			for(HTableDescriptor d : admin.listTables()) {
				byte[] p = Bytes.add(prefix, IDX_TABLE);
				if(Bytes.startsWith(d.getName(), p)) {
					byte[] k = Bytes.tail(d.getName(), d.getName().length - p.length);
					idxTables.put(tableUnescape(k), d.getName());
					log.trace("{} loaded index table {}", this, d.getNameAsString());
				}
			}
		} finally {
			admin.close();
		}
	}
	
	public void dropTables() throws IOException {
		log.info("{} dropping tables", this);
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			if(admin.tableExists(vtxTable)) {
				if(!admin.isTableDisabled(vtxTable))
					admin.disableTable(vtxTable);
				log.debug("{} dropping table {}", this, Bytes.toString(vtxTable));
				admin.deleteTable(vtxTable);
			}
			
			if(admin.tableExists(vtxPropertiesTable)) {
				if(!admin.isTableDisabled(vtxPropertiesTable))
					admin.disableTable(vtxPropertiesTable);
				log.debug("{} dropping table {}", this, Bytes.toString(vtxPropertiesTable));
				admin.deleteTable(vtxPropertiesTable);
			}
			
			if(admin.tableExists(edgTable)) {
				if(!admin.isTableDisabled(edgTable))
					admin.disableTable(edgTable);
				log.debug("{} dropping table {}", this, Bytes.toString(edgTable));
				admin.deleteTable(edgTable);
			}
			
			if(admin.tableExists(edgPropertiesTable)) {
				if(!admin.isTableDisabled(edgPropertiesTable))
					admin.disableTable(edgPropertiesTable);
				log.debug("{} dropping table {}", this, Bytes.toString(edgPropertiesTable));
				admin.deleteTable(edgPropertiesTable);
			}
			
			for(HTableDescriptor d : admin.listTables()) {
				if(Bytes.startsWith(d.getName(), Bytes.add(prefix, IDX_TABLE))) {
					if(!admin.isTableDisabled(d.getName()))
						admin.disableTable(d.getName());
					log.debug("{} dropping table {}", this, d.getNameAsString());
					admin.deleteTable(d.getName());
				}
			}
		} finally {
			admin.close();
		}
		log.debug("{} tables dropped", this);
	}
	
	public void shutdown() throws IOException {
		log.info("{} shutdown", this);
		_pool.close();
	}
	
	public byte[] addVertex(byte[] vid) throws IOException {
		if(vid == null) {
			vid = new byte[DEFAULT_ID_LENGTH];
			random.nextBytes(vid);
		}
		HTableInterface table = table(vtxTable);
		try {
			Put p = new Put(vid);
			p.add(VTX_CF, VTX_IS_Q, TRUE);
			table.put(p);
		} finally {
			repool(table);
		}
		return vid;
	}
	
	public boolean vertexExists(byte[] vid) throws IOException {
		HTableInterface table = table(vtxTable);
		try {
			Get g = new Get(vid);
			g.setMaxVersions(1);
			g.addColumn(VTX_CF, VTX_IS_Q);
			return table.get(g).getValue(VTX_CF, VTX_IS_Q) != null;
		} finally {
			repool(table);
		}
	}
	
	public Iterable<byte[]> getAllVertices() {
		return new Iterable<byte[]>() {
			@Override
			public Iterator<byte[]> iterator() {
				Scan scan = new Scan();
				scan.addFamily(VTX_CF);
				scan.setBatch(8192);
				scan.setCaching(8192);
				scan.setMaxVersions(1);
				final HTableInterface table = table(vtxTable);
				ResultScanner scanner;
				try {
					scanner = table.getScanner(scan);
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				final Iterator<Result> sci = scanner.iterator();
				
				return new Iterator<byte[]>() {
					boolean closed;
					
					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
					
					@Override
					public byte[] next() {
						if(!hasNext())
							throw new NoSuchElementException();
						try {
							return sci.next().getRow();
						} finally {
							hasNext();
						}
					}
					
					@Override
					public boolean hasNext() {
						if(!sci.hasNext() && !closed) {
							closed = true;
							try {
								repool(table);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						}
						return sci.hasNext();
					}
				};
			}
		};
	}
	
	public void removeVertex(byte[] vid) throws IOException {
		for(byte[][] e : getEdgesOut(vid))
			removeEdge(e[1], e[0], e[2]);
		for(byte[][] e : getEdgesIn(vid))
			removeEdge(e[1], e[0], e[2]);
		removeVertexProperties(vid);
		HTableInterface table = table(vtxTable);
		try {
			Delete d = new Delete(vid);
			table.delete(d);
		} finally {
			repool(table);
		}
	}
	
	public byte[] addEdge(byte[] eid, byte[] vout, byte[] vin) throws IOException {
		if(eid == null) {
			eid = new byte[DEFAULT_ID_LENGTH];
			random.nextBytes(eid);
		}
		HTableInterface table = table(edgTable);
		try {
			Put p = new Put(eid);
			p.add(EDG_CF, EDG_IS_Q, TRUE);
			p.add(EDG_CF, EDG_OUT_Q, vout);
			p.add(EDG_CF, EDG_IN_Q, vin);
			table.put(p);
		} finally {
			repool(table);
		}
		table = table(vtxTable);
		try {
			Put p = new Put(Bytes.add(vout, eid));
			p.add(VTX_OUT_CF, vout, vin);
			table.put(p);
			p = new Put(Bytes.add(vin, eid));
			p.add(VTX_IN_CF, vin, vout);
			table.put(p);
		} finally {
			repool(table);
		}
		return eid;
	}
	
	public boolean edgeExists(byte[] eid) throws IOException {
		HTableInterface table = table(edgTable);
		try {
			Get g = new Get(eid);
			g.setMaxVersions(1);
			g.addColumn(EDG_CF, EDG_IS_Q);
			return table.get(g).getValue(EDG_CF, EDG_IS_Q) != null;
		} finally {
			repool(table);
		}
	}
	
	public Iterable<byte[]> getAllEdges() {
		return new Iterable<byte[]>() {
			@Override
			public Iterator<byte[]> iterator() {
				Scan scan = new Scan();
				scan.addFamily(EDG_CF);
				scan.setBatch(8192);
				scan.setCaching(8192);
				scan.setMaxVersions(1);
				final HTableInterface table = table(edgTable);
				ResultScanner scanner;
				try {
					scanner = table.getScanner(scan);
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				final Iterator<Result> sci = scanner.iterator();
				
				return new Iterator<byte[]>() {
					boolean closed;
					
					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
					
					@Override
					public byte[] next() {
						if(!hasNext())
							throw new NoSuchElementException();
						try {
							return sci.next().getRow();
						} finally {
							hasNext();
						}
					}
					
					@Override
					public boolean hasNext() {
						if(!sci.hasNext() && !closed) {
							closed = true;
							try {
								repool(table);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						}
						return sci.hasNext();
					}
				};
			}
		};
	}
	
	public void removeEdge(byte[] eid) throws IOException {
		HTableInterface table = table(edgTable);
		byte[] vout, vin;
		try {
			Get g = new Get(eid);
			g.addFamily(EDG_CF);
			g.setMaxVersions(1);
			Result r = table.get(g);
			vout = r.getValue(EDG_CF, EDG_OUT_Q);
			vin = r.getValue(EDG_CF, EDG_IN_Q);
		} finally {
			repool(table);
		}
		removeEdge(eid, vout, vin);
	}
	
	public void removeEdge(byte[] eid, byte[] vout, byte[] vin) throws IOException {
		removeEdgeProperties(eid);
		HTableInterface table = table(edgTable);
		try {
			Delete d = new Delete(eid);
			table.delete(d);
		} finally {
			repool(table);
		}
		table = table(vtxTable);
		try {
			Delete d = new Delete(Bytes.add(vout, eid));
			d.deleteColumn(VTX_OUT_CF, vout);
			table.delete(d);
			d = new Delete(Bytes.add(vin, eid));
			d.deleteColumn(VTX_IN_CF, vin);
			table.delete(d);
		} finally {
			repool(table);
		}
	}
	
	public byte[] getOutVertex(byte[] eid) throws IOException {
		HTableInterface table = table(edgTable);
		try {
			Get g = new Get(eid);
			g.addColumn(EDG_CF, EDG_OUT_Q);
			g.setMaxVersions(1);
			Result r = table.get(g);
			return r.getValue(EDG_CF, EDG_OUT_Q);
		} finally {
			repool(table);
		}
	}
	
	public byte[] getInVertex(byte[] eid) throws IOException {
		HTableInterface table = table(edgTable);
		try {
			Get g = new Get(eid);
			g.addColumn(EDG_CF, EDG_IN_Q);
			g.setMaxVersions(1);
			Result r = table.get(g);
			return r.getValue(EDG_CF, EDG_IN_Q);
		} finally {
			repool(table);
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
				scan.setMaxVersions(1);
				ResultScanner scanner;
				final HTableInterface table = table(vtxTable);
				try {
					scanner = table.getScanner(scan);
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				final Iterator<Result> sci = scanner.iterator();
				
				return new Iterator<byte[][]>() {
					boolean closed;
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
						try {
							return n;
						} finally {
							hasNext();
						}
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
						if(next == null && !closed) {
							closed = true;
							try {
								repool(table);
							} catch(IOException e) {
								throw new RuntimeException(e);
							}
						}
						return next != null;
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
				scan.setMaxVersions(1);
				ResultScanner scanner;
				final HTableInterface table = table(vtxTable);
				try {
					scanner = table.getScanner(scan);
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				final Iterator<Result> sci = scanner.iterator();
				
				return new Iterator<byte[][]>() {
					boolean closed;
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
						try {
							return n;
						} finally {
							hasNext();
						}
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
						if(next == null && !closed) {
							closed = true;
							try {
								repool(table);
							} catch(IOException e) {
								throw new RuntimeException(e);
							}
						}
						return next != null;
					}
				};
			}
		};
	}
	
	public void setVertexProperty(byte[] vid, byte[] pkey, byte[] pval) throws IOException {
		removeVertexProperty(vid, pkey);
		
		HTableInterface table = table(vtxPropertiesTable);
		try {
			Put p = new Put(vid);
			p.add(VTXP_CF, pkey, pval);
			table.put(p);
		} finally {
			repool(table);
		}
		
		if(idxTables.containsKey(pkey)) {
			table = table(idxTables.get(pkey));
			try {
				Put p = new Put(Bytes.add(pval, vid));
				p.add(IDX_VTX_CF, pval, vid);
				table.put(p);
			} finally {
				repool(table);
			}
		}
	}
	
	public byte[] getVertexProperty(byte[] vid, byte[] pkey) throws IOException {
		HTableInterface table = table(vtxPropertiesTable);
		try {
			Get g = new Get(vid);
			g.addColumn(VTXP_CF, pkey);
			g.setMaxVersions(1);
			Result r = table.get(g);
			return r.getValue(VTXP_CF, pkey);
		} finally {
			repool(table);
		}
	}
	
	public void removeVertexProperty(byte[] vid, byte[] pkey) throws IOException {
		byte[] pval = null;
		if(idxTables.containsKey(pkey))
			pval = getVertexProperty(vid, pkey);
		
		HTableInterface table = table(vtxPropertiesTable);
		try {
			Delete d = new Delete(vid);
			d.deleteColumn(VTXP_CF, pkey);
			table.delete(d);
		} finally {
			repool(table);
		}
		
		if(idxTables.containsKey(pkey) && pval != null) {
			table = table(idxTables.get(pkey));
			try {
				Delete d = new Delete(Bytes.add(pval, vid));
				d.deleteColumn(IDX_VTX_CF, pval);
				table.delete(d);
			} finally {
				repool(table);
			}
		}
	}
	
	public void removeVertexProperty(byte[] vid, byte[] pkey, byte[] pval) throws IOException {
		HTableInterface table = table(vtxPropertiesTable);
		try {
			Delete d = new Delete(vid);
			d.deleteColumn(VTXP_CF, pkey);
			table.delete(d);
		} finally {
			repool(table);
		}
		
		if(idxTables.containsKey(pkey)) {
			table = table(idxTables.get(pkey));
			try {
				Delete d = new Delete(Bytes.add(pval, vid));
				d.deleteColumn(IDX_VTX_CF, pval);
				table.delete(d);
			} finally {
				repool(table);
			}
		}
	}
	
	public void removeVertexProperties(byte[] vid) throws IOException {
		for(byte[][] p : getVertexProperties(vid))
			removeVertexProperty(vid, p[0], p[1]);
		HTableInterface table = table(vtxPropertiesTable);
		try {
			Delete d = new Delete(vid);
			table.delete(d);
		} finally {
			repool(table);
		}
	}
	
	public Iterable<byte[][]> getVertexProperties(final byte[] vid) throws IOException {
		return new Iterable<byte[][]>() {
			@Override
			public Iterator<byte[][]> iterator() {
				List<byte[][]> ret = new ArrayList<>();
				try {
					HTableInterface table = table(vtxPropertiesTable);
					try {
						Get g = new Get(vid);
						g.addFamily(VTXP_CF);
						g.setMaxVersions(1);
						Result r = table.get(g);
						if(r.getFamilyMap(VTXP_CF) != null)
							for(byte[] pkey : r.getFamilyMap(VTXP_CF).keySet())
								ret.add(new byte[][] {pkey, r.getValue(VTXP_CF, pkey)});
					} finally {
						repool(table);
					}
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				return ret.iterator();
			}
		};
	}

	public void setEdgeProperty(byte[] eid, byte[] pkey, byte[] pval) throws IOException {
		removeEdgeProperty(eid, pkey);
		
		HTableInterface table = table(edgPropertiesTable);
		try {
			Put p = new Put(eid);
			p.add(EDGP_CF, pkey, pval);
			table.put(p);
		} finally {
			repool(table);
		}

		if(idxTables.containsKey(pkey)) {
			table = table(idxTables.get(pkey));
			try {
				Put p = new Put(Bytes.add(pval, eid));
				p.add(IDX_EDG_CF, pval, eid);
				table.put(p);
			} finally {
				repool(table);
			}
		}
	}
	
	public byte[] getEdgeProperty(byte[] eid, byte[] pkey) throws IOException {
		HTableInterface table = table(edgPropertiesTable);
		try {
			Get g = new Get(eid);
			g.addColumn(EDGP_CF, pkey);
			g.setMaxVersions(1);
			Result r = table.get(g);
			return r.getValue(EDGP_CF, pkey);
		} finally {
			repool(table);
		}
	}
	
	public void removeEdgeProperty(byte[] eid, byte[] pkey) throws IOException {
		byte[] pval = null;
		if(idxTables.containsKey(pkey))
			pval = getEdgeProperty(eid, pkey);
		
		HTableInterface table = table(edgPropertiesTable);
		try {
			Delete d = new Delete(eid);
			d.deleteColumn(EDGP_CF, pkey);
			table.delete(d);
		} finally {
			repool(table);
		}

		if(idxTables.containsKey(pkey) && pval != null) {
			table = table(idxTables.get(pkey));
			try {
				Delete d = new Delete(Bytes.add(pval, eid));
				d.deleteColumn(IDX_EDG_CF, pval);
				table.delete(d);
			} finally {
				repool(table);
			}
		}
	}
	
	public void removeEdgeProperty(byte[] eid, byte[] pkey, byte[] pval) throws IOException {
		HTableInterface table = table(edgPropertiesTable);
		try {
			Delete d = new Delete(eid);
			d.deleteColumn(EDGP_CF, pkey);
			table.delete(d);
		} finally {
			repool(table);
		}

		if(idxTables.containsKey(pkey)) {
			table = table(idxTables.get(pkey));
			try {
				Delete d = new Delete(Bytes.add(pval, eid));
				d.deleteColumn(IDX_EDG_CF, pval);
				table.delete(d);
			} finally {
				repool(table);
			}
		}
	}
	
	public void removeEdgeProperties(byte[] eid) throws IOException {
		for(byte[][] p : getEdgeProperties(eid))
			removeEdgeProperty(eid, p[0], p[1]);
		HTableInterface table = table(edgPropertiesTable);
		try {
			Delete d = new Delete(eid);
			table.delete(d);
		} finally {
			repool(table);
		}
	}
	
	public Iterable<byte[][]> getEdgeProperties(final byte[] eid) throws IOException {
		return new Iterable<byte[][]>() {
			@Override
			public Iterator<byte[][]> iterator() {
				List<byte[][]> ret = new ArrayList<>();
				try {
					HTableInterface table = table(edgPropertiesTable);
					try {
						Get g = new Get(eid);
						g.addFamily(EDGP_CF);
						g.setMaxVersions(1);
						Result r = table.get(g);
						if(r.getFamilyMap(EDGP_CF) != null)
							for(byte[] pkey : r.getFamilyMap(EDGP_CF).keySet())
								ret.add(new byte[][] {pkey, r.getValue(EDGP_CF, pkey)});
					} finally {
						repool(table);
					}
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				return ret.iterator();
			}
		};
	}

	public void createIndex(byte[] pkey) throws IOException {
		byte[] tkey = tableEscape(pkey);
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		try {
			HTableDescriptor d = new HTableDescriptor(Bytes.add(prefix, IDX_TABLE, tkey));
			log.info("{} creating index table {}", this, d.getNameAsString());
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
			log.info("{} dropping index table {}", this, Bytes.toString(idxTables.get(pkey)));
			admin.disableTable(idxTables.get(pkey));
			admin.deleteTable(idxTables.get(pkey));
			idxTables.remove(pkey);
		} finally {
			admin.close();
		}
	}
	
	public Set<byte[]> getIndexKeys() {
		return Collections.unmodifiableSet(idxTables.keySet());
	}
	
	public void reindexVertices(byte[] pkey) throws IOException {
		log.info("{} reindexing vertex property {}", this, Bytes.toStringBinary(pkey));
		for(byte[] vid : getAllVertices()) {
			byte[] pval = getVertexProperty(vid, pkey);
			if(pval != null)
				setVertexProperty(vid, pkey, pval);
		}
	}
	
	public void reindexEdges(byte[] pkey) throws IOException {
		log.info("{} reindexing edge property {}", this, Bytes.toStringBinary(pkey));
		for(byte[] vid : getAllEdges()) {
			byte[] pval = getEdgeProperty(vid, pkey);
			if(pval != null)
				setEdgeProperty(vid, pkey, pval);
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
				scan.setMaxVersions(1);
				ResultScanner scanner;
				final HTableInterface table = table(idxTables.get(pkey));
				try {
					scanner = table.getScanner(scan);
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				final Iterator<Result> sci = scanner.iterator();
				
				return new Iterator<byte[][]>() {
					boolean closed;
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
						try {
							return n;
						} finally {
							hasNext();
						}
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
						if(next == null && !closed) {
							closed = true;
							try {
								repool(table);
							} catch(IOException e) {
								throw new RuntimeException(e);
							}
						}
						return next != null;
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
				scan.setMaxVersions(1);
				ResultScanner scanner;
				final HTableInterface table = table(idxTables.get(pkey));
				try {
					scanner = table.getScanner(scan);
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				final Iterator<Result> sci = scanner.iterator();
				
				return new Iterator<byte[][]>() {
					boolean closed;
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
						try {
							return n;
						} finally {
							hasNext();
						}
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
						if(next == null && !closed) {
							closed = true;
							try {
								repool(table);
							} catch(IOException e) {
								throw new RuntimeException(e);
							}
						}
						return next != null;
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
				scan.setMaxVersions(1);
				ResultScanner scanner;
				final HTableInterface table = table(idxTables.get(pkey));
				try {
					scanner = table.getScanner(scan);
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				final Iterator<Result> sci = scanner.iterator();
				
				return new Iterator<byte[][]>() {
					boolean closed;
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
						try {
							return n;
						} finally {
							hasNext();
						}
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
						if(next == null && !closed) {
							closed = true;
							try {
								repool(table);
							} catch(IOException e) {
								throw new RuntimeException(e);
							}
						}
						return next != null;
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
				scan.setMaxVersions(1);
				ResultScanner scanner;
				final HTableInterface table = table(idxTables.get(pkey));
				try {
					scanner = table.getScanner(scan);
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
				final Iterator<Result> sci = scanner.iterator();
				
				return new Iterator<byte[][]>() {
					boolean closed;
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
						try {
							return n;
						} finally {
							hasNext();
						}
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
						if(next == null && !closed) {
							closed = true;
							try {
								repool(table);
							} catch(IOException e) {
								throw new RuntimeException(e);
							}
						}
						return next != null;
					}
				};
			}
		};
	}

	public byte[] getPrefix() {
		return prefix;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + "[" + Bytes.toString(prefix) + "]";
	}
}
