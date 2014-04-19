package org.rbkluster.hgraph;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import org.apache.hadoop.hbase.HConstants;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastInput;
import com.esotericsoftware.kryo.io.FastOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

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
	
	public static byte[] toKryoBytes(Object o) {
		Kryo kryo = new Kryo();
		kryo.setRegistrationRequired(false);
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		Output output = new FastOutput(bout);
		kryo.writeClassAndObject(output, o);
		output.flush();
		return bout.toByteArray();
	}
	
	public static Object fromKryoBytes(byte[] b) {
		Kryo kryo = new Kryo();
		kryo.setRegistrationRequired(false);
		Input input = new FastInput(b);
		return kryo.readClassAndObject(input);
	}
	
	private GBytes() {}
}
