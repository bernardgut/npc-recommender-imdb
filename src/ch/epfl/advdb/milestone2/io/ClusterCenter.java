/*
 * BERNARD GUTERMANN (c) 2013
 */
package ch.epfl.advdb.milestone2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This abstraction represents a Cluster Centroid. It is used for both IMDB and Netflix cluster
 * centroids. It uses an underlying TreeMap structure to maintain order in the dimensions. This is to
 * ensure that the overriden compareTo function works flawlessly
 * @author Bernad Gütermann
 *
 */
public class ClusterCenter extends TreeMap<Integer,Float> implements
		WritableComparable<ClusterCenter> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2707896690769120709L;
	
	private int clusterID;
	public ClusterCenter(){
		super();
	}
	
	public ClusterCenter(int clusterID) {
		super();
		this.clusterID=clusterID;
	}
	

	public ClusterCenter(Map<? extends Integer, ? extends Float> m, int clusterID) {
		super(m);
		this.clusterID=clusterID;
	}

	public ClusterCenter(Text value) {
		String cc[] = value.toString().split(":")[1].split(";");
		String[] keyValue;
		for (String kv : cc){
			keyValue = kv.split(",");
			this.put(Integer.valueOf(keyValue[0]), Float.valueOf(keyValue[1]));
		}
		clusterID=Integer.valueOf(value.toString().split(":")[0]);
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput arg0) throws IOException {
		int size = arg0.readInt();
		clusterID = arg0.readInt();
		this.clear();
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
		arg0.writeInt(size());
		arg0.writeInt(clusterID);
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
		return (this.toString().compareTo(o.toString()));
	}
	
	

	/* (non-Javadoc)
	 * @see java.util.AbstractMap#toString()
	 */
	@Override
	public String toString() {
		StringBuilder out =new StringBuilder().append(clusterID).append(":");
		for (Entry<Integer, Float> e : this.entrySet()){
			out.append(e.getKey()).append(",").append(e.getValue()).append(";");
		}
		return out.toString();
	}
	
	/**
	 * @return id of this ClusterCenter instance
	 */
	public int getClusterID(){
		return clusterID;
	}

	/**
	 * Addition between a binary FeatureVector of indexes and this ClusterCenter vector
	 * @param f FeatureVector to be added
	 * @throws IOException 
	 */
	public void add(FVector f) throws IOException {
		if(f instanceof FVectorIMDB){
			for(float index : f){
				if(this.get((int)index)==null)
					this.put((int)index, 1f);
				else 
					this.put((int)index,this.get((int)index)+1f);
			}
		}
		else if (f instanceof FVectorNetflix){
			for (int i=0;i<f.size();++i){	//10
				if(this.get(i)==null)
					this.put(i, f.get(i));
				else
					this.put(i, this.get(i)+f.get(i));
			}
		}
		else throw new IOException("Invalid FVector subclass.");
	}

	/**
	 * Division between this VlusterCenter vector and a float 
	 * @param count float precision denominator
	 */
	public void divide(float count) {
		if(count==0) throw new IllegalArgumentException("divide by 0");
		for(Entry<Integer,Float> e : this.entrySet()){
			this.put(e.getKey(), e.getValue()/count);
		}
	}
	
	
}
