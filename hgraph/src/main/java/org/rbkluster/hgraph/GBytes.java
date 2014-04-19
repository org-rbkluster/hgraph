package org.rbkluster.hgraph;

import java.util.Arrays;

import org.apache.hadoop.hbase.HConstants;

public class GBytes {
	public static byte[] endKey(byte[] b) {
		byte[] r = Arrays.copyOf(b, b.length);
		int i = r.length;
		do {
			if(--i < 0)
				break;
			r[i]++;
		} while(r[i] == 0);
		if(i < 0)
			return HConstants.EMPTY_END_ROW;
		return r;
	}
	
	private GBytes() {}
}
