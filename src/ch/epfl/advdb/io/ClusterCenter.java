package ch.epfl.advdb.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;

public class ClusterCenter extends HashMap<Integer,Float> implements
		WritableComparable<ClusterCenter> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2707896690769120709L;
	
	public ClusterCenter() {
		super();
	}

	public ClusterCenter(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	public ClusterCenter(int initialCapacity) {
		super(initialCapacity);
	}

	public ClusterCenter(Map<? extends Integer, ? extends Float> m) {
		super(m);
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput arg0) throws IOException {
		int size = arg0.readInt();
//		HashMap<Integer, Float> h = new HashMap<Integer, Float>(size);
		for (int i=0;i<size;++i){
			this.put(arg0.readInt(),arg0.readFloat());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(this.size());
		for(java.util.Map.Entry<Integer, Float> e : this.entrySet()){
			arg0.writeInt(e.getKey());
			arg0.writeFloat(e.getValue());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(ClusterCenter o) {
		//No absolute order for vectors, only look for equality
		for (int i : this.keySet()) {
			double c = this.get(i) - o.get(i);
			if (c != 0.0f) 
				return 1;
		}
		return 0;
	}
}
