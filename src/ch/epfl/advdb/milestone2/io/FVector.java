/**
 * 
 */
package ch.epfl.advdb.milestone2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author mint05
 *
 */
public abstract class FVector extends ArrayList<Float> implements
		WritableComparable<FVector> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1703005846827607662L;
	private int movieID;
	/**
	 * 
	 */
	public FVector() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * 
	 * @param movieID
	 */
	public FVector(int movieID){
		super();
		this.movieID=movieID;
	}

	/**
	 * @param initialCapacity
	 */
	public FVector(int initialCapacity, int movieID) {
		super(initialCapacity);
		this.movieID=movieID;
	}

	/**
	 * @param c
	 */
	public FVector(Collection<? extends Float> c, int movieID) {
		super(c);
		this.movieID=movieID;
	}

	public FVector(Text value){
		String cc[] = value.toString().split(",");
		for (int i =1;i<cc.length;++i){
			this.add(Float.valueOf(cc[i]));
		}
		this.movieID=Integer.valueOf(cc[0]);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput arg0) throws IOException {
		int size = arg0.readInt();
		this.movieID=arg0.readInt();
		this.ensureCapacity(size);
		for (int i=0;i<size;++i){
			this.add(arg0.readFloat());
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(this.size());
		arg0.writeInt(this.movieID);
		for(int i=0; i<this.size();++i){
			arg0.writeFloat(this.get(i));
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(FVector o) {
		//No absolute order for vectors, only look for equality
		return (this.toString().compareTo(o.toString()));
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString(){
		String out=this.movieID+",";
		for(int i=0;i<this.size();++i){
			out+=this.get(i)+",";
		}
		return out;
	}
	
	public int getId(){
		return this.movieID;
	}
	
	/**
	 * Distance function : uses cosine similarity between a clusterCenter vector and this feature
	 * @param c
	 * @return
	 */
	abstract public float getDistance(ClusterCenter c);
	
}
