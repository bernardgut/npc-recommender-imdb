package ch.epfl.advdb.milestone2.io;

import java.util.Collection;

import org.apache.hadoop.io.Text;

public class FVectorIMDB extends FVector{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3285319392520018214L;

	public FVectorIMDB() {
		super();
	}
	
	public FVectorIMDB(int movieID) {
		super(movieID);
	}

	public FVectorIMDB(Collection<? extends Float> c, int movieID) {
		super(c, movieID);
	}

	public FVectorIMDB(int initialCapacity, int movieID) {
		super(initialCapacity, movieID);
	}

	public FVectorIMDB(Text value) {
		super(value);
	}
	
	public FVectorIMDB(String value){
		super(value);
	}

	/* (non-Javadoc)
	 * @see ch.epfl.advdb.milestone2.io.FVector#getDistance(ch.epfl.advdb.milestone2.io.ClusterCenter)
	 */
	@Override
	public float getDistance(ClusterCenter c){
		float sum=0;
		float nv = 0;
		float nc=0;
		for (float index : this){
			if(c.get((int)index)!=null)
				//numerator
				sum+=c.get((int)index);
			//sum Ai squared
			nv++;
		}
		//sum Bi squared
		for(float val : c.values())
			nc+=val*val;
		if(nc==0||nv==0)
			return 0;
		else return (float) (1-(sum/(Math.sqrt(nv)*Math.sqrt(nc))));
	}

	/*
	 * (non-Javadoc)
	 * @see ch.epfl.advdb.milestone2.io.FVector#getDistance(ch.epfl.advdb.milestone2.io.FVector)
	 */
	@Override
	public float getDistance(FVector s) {
		float sum=0;
		float nv = 0;
		float ns=0;
		for (float index : this){
			if (s.contains(index))
				sum+=1;
			nv++;
		}
		for (float ind : s){
			ns++;
		}
		if(ns==0||nv==0)
			return 0;
		else return (float) (1-(sum/(Math.sqrt(nv)*Math.sqrt(ns))));
	}
}