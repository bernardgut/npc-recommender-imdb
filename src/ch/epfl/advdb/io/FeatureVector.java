package ch.epfl.advdb.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.hadoop.io.WritableComparable;

public class FeatureVector extends ArrayList<Integer> implements WritableComparable<FeatureVector> {


	/**
	 * 
	 */
	private static final long serialVersionUID = -3285319392520018214L;

	public FeatureVector() {
		super();
	}

	public FeatureVector(Collection<? extends Integer> c) {
		super(c);
	}

	public FeatureVector(int initialCapacity) {
		super(initialCapacity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput arg0) throws IOException {
		int size = arg0.readInt();
		this.ensureCapacity(size);
		for (int i=0;i<size;++i){
			this.add(arg0.readInt());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.write(this.size());
		for(int i=0; i<this.size();++i){
			arg0.write(this.get(i));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(FeatureVector arg0) {
		//No absolute order for vectors, only look for equality
		for (int i = 0; i < this.size(); i++) {
			int c = this.get(i) - arg0.get(i);
			if (c != 0) 
				return 1;
		}
		return 0;
	}
}