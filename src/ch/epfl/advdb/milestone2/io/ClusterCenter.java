package ch.epfl.advdb.milestone2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ClusterCenter extends TreeMap<Integer,Double> implements
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
	

	public ClusterCenter(Map<? extends Integer, ? extends Double> m, int clusterID) {
		super(m);
		this.clusterID=clusterID;
	}

	public ClusterCenter(Text value) {
		String cc[] = value.toString().split(":")[1].split(";");
		String[] keyValue;
		for (String kv : cc){
			keyValue = kv.split(",");
			this.put(Integer.valueOf(keyValue[0]), Double.valueOf(keyValue[1]));
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
			this.put(arg0.readInt(),arg0.readDouble());
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
		for(java.util.Map.Entry<Integer, Double> e : this.entrySet()){
			arg0.writeInt(e.getKey());
			arg0.writeDouble(e.getValue());
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
		String p=String.valueOf(clusterID)+":";
		for (Entry<Integer, Double> e : this.entrySet()){
			p+=e.getKey().toString()+","+e.getValue().toString()+";";
		}
		return p;
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
	 */
	public void add(FeatureVector f) {
		for(int index : f){
			if(this.get(index)==null)
				this.put(index, 1.0);
			else 
				this.put(index,this.get(index)+1.0);
		}
	}

	/**
	 * Division between this VlusterCenter vector and a double 
	 * @param count double precision denominator
	 */
	public void divide(double count) {
		for(Entry<Integer,Double> e : this.entrySet()){
			this.put(e.getKey(), e.getValue()/count);
		}
	}
	
	
}
