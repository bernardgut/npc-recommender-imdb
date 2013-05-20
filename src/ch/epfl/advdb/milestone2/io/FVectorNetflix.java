/*
 * BERNARD GUTERMANN (c) 2013
 */
package ch.epfl.advdb.milestone2.io;

import java.util.Collection;

import org.apache.hadoop.io.Text;

/**
 * This Class represents a Netflix Feature Vector. 
 * The valeus stored are the 10 scores values, one for each dimension
 * @author Bernard Gütermann
 *
 */
public class FVectorNetflix extends FVector {

	private static final long serialVersionUID = 2319152078419860556L;

	public FVectorNetflix() {
	}

	public FVectorNetflix(int movieID) {
		super(movieID);
	}

	public FVectorNetflix(int initialCapacity, int movieID) {
		super(initialCapacity, movieID);
	}

	public FVectorNetflix(Collection<? extends Float> c, int movieID) {
		super(c, movieID);
	}

	public FVectorNetflix(Text value) {
		super(value);
	}
	
	public FVectorNetflix(String value){
		super(value);
	}

	/* (non-Javadoc)
	 * @see ch.epfl.advdb.milestone2.io.FVector#getDistance(ch.epfl.advdb.milestone2.io.ClusterCenter)
	 */
	@Override
	public float getDistance(ClusterCenter c) {
		float sum=0;
		float nv = 0;
		float nc=0;
		float a, b;
		//iterate over the 10 values
		for (int i = 0; i<this.size();++i){
			a = this.get(i);
			if(c.get(i)==null)
				b = 0;
			else 
				b= c.get(i);
			//numerator
			sum+=a*b;
			//sum Ai & Bi squared
			nv+=a*a;
			nc+=b*b;
		}
		//sum Bi squared
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
		float a, b;
		//iterate over the 10 values
		for (int i = 0; i<this.size();++i){
			a = this.get(i);
			b= s.get(i);
			//numerator
			sum+=a*b;
			//sum Ai & Bi squared
			nv+=a*a;
			ns+=b*b;
		}
		//sum Bi squared
		if(ns==0||nv==0)
			return 0;
		else return (float) (1-(sum/(Math.sqrt(nv)*Math.sqrt(ns))));
	}
}
