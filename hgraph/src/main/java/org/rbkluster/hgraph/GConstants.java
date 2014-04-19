package org.rbkluster.hgraph;

import org.apache.hadoop.hbase.util.Bytes;

public class GConstants {
	
	public static final byte[] VTX_TABLE = Bytes.toBytes("_vtx");
	public static final byte[] VTXP_TABLE = Bytes.toBytes("_vtxp");
	public static final byte[] EDG_TABLE = Bytes.toBytes("_edg");
	public static final byte[] EDGP_TABLE = Bytes.toBytes("_edgp");
	public static final byte[] IDX_TABLE = Bytes.toBytes("_idx_");
	public static final byte[] IDXS_TABLE = Bytes.toBytes("_idxs");
	
	public static final byte[] VTX_CF = Bytes.toBytes("vtx");
	public static final byte[] VTX_IS_Q = Bytes.toBytes("is");
	public static final byte[] VTX_OUT_CF = Bytes.toBytes("out");
	public static final byte[] VTX_IN_CF = Bytes.toBytes("in");
	
	public static final byte[] VTXP_CF = Bytes.toBytes("vtxp");
	
	public static final byte[] EDG_CF = Bytes.toBytes("edg");
	public static final byte[] EDG_IS_Q = Bytes.toBytes("is");
	public static final byte[] EDG_OUT_Q = Bytes.toBytes("out");
	public static final byte[] EDG_IN_Q = Bytes.toBytes("in");
	
	public static final byte[] EDGP_CF = Bytes.toBytes("edgp");
	
	public static final byte[] IDX_VTX_CF = Bytes.toBytes("vtx_idx");
	public static final byte[] IDX_EDG_CF = Bytes.toBytes("edg_idx");
	
	public static final byte[] IDXS_CF = Bytes.toBytes("idxs");
	public static final byte[] IDXS_IS_VTX_Q = Bytes.toBytes("is_vtx");
	public static final byte[] IDXS_IS_EDG_Q = Bytes.toBytes("is_edg");
	
	public static final byte[] TRUE = Bytes.toBytes(true);
	public static final byte[] FALSE = Bytes.toBytes(false);
	
	private GConstants() {}
}
