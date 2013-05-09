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

	public FVectorIMDB(Collection<? extends Integer> c, int movieID) {
		super(c, movieID);
	}

	public FVectorIMDB(int initialCapacity, int movieID) {
		super(initialCapacity, movieID);
	}

	public FVectorIMDB(Text value) {
		super(value);
	}

	/* (non-Javadoc)
	 * @see ch.epfl.advdb.milestone2.io.FVector#getDistance(ch.epfl.advdb.milestone2.io.ClusterCenter)
	 */
	@Override
	public double getDistance(ClusterCenter c){
		double sum=0;
		double nv = 0;
		double nc=0;
		for (int index : this){
			if(c.get(index)!=null)
				//numerator
				sum+=c.get(index);
			//sum Ai squared
			nv++;
		}
		//sum Bi squared
		for(double val : c.values())
			nc+=val*val;
		if(nc==0||nv==0)
			return 0;
		else return 1-(sum/(Math.sqrt(nv)*Math.sqrt(nc)));
	}
}