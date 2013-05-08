package ch.epfl.advdb.milestone2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FeatureVector extends ArrayList<Integer> implements WritableComparable<FeatureVector> {


	/**
	 * 
	 */
	private static final long serialVersionUID = -3285319392520018214L;
	
	private int movieID;
	
	public FeatureVector() {
		super();
	}
	public FeatureVector(int movieID) {
		super();
		this.movieID=movieID;
	}

	public FeatureVector(Collection<? extends Integer> c, int movieID) {
		super(c);
		this.movieID=movieID;
	}

	public FeatureVector(int initialCapacity, int movieID) {
		super(initialCapacity);
		this.movieID=movieID;
	}

	public FeatureVector(Text value) {
		String cc[] = value.toString().split(",");
		for (int i =1;i<cc.length;++i){
			this.add(Integer.valueOf(cc[i]));
		}
		this.movieID=Integer.valueOf(cc[0]);
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
		arg0.writeInt(this.size());
		for(int i=0; i<this.size();++i){
			arg0.writeInt(this.get(i));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(FeatureVector o) {
		//No absolute order for vectors, only look for equality
		return (this.toString().compareTo(o.toString()));
	}
	/* (non-Javadoc)
	 * @see java.util.AbstractCollection#toString()
	 */
	@Override
	public String toString() {
		String out=this.movieID+",";
		for(int i=0;i<this.size();++i){
			out+=this.get(i);
		}
		return out;
	}
	
	
}