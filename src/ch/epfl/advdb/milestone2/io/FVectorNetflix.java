/**
 * 
 */
package ch.epfl.advdb.milestone2.io;

import java.util.Collection;

import org.apache.hadoop.io.Text;

/**
 * @author mint05
 *
 */
public class FVectorNetflix extends FVector {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2319152078419860556L;

	/**
	 * 
	 */
	public FVectorNetflix() {
	}

	/**
	 * @param movieID
	 */
	public FVectorNetflix(int movieID) {
		super(movieID);
	}

	/**
	 * @param initialCapacity
	 * @param movieID
	 */
	public FVectorNetflix(int initialCapacity, int movieID) {
		super(initialCapacity, movieID);
	}

	/**
	 * @param c
	 * @param movieID
	 */
	public FVectorNetflix(Collection<? extends Integer> c, int movieID) {
		super(c, movieID);
	}

	/**
	 * @param value
	 */
	public FVectorNetflix(Text value) {
		super(value);
	}

	/* (non-Javadoc)
	 * @see ch.epfl.advdb.milestone2.io.FVector#getDistance(ch.epfl.advdb.milestone2.io.ClusterCenter)
	 */
	@Override
	public double getDistance(ClusterCenter c) {
		double sum=0;
		double nv = 0;
		double nc=0;
		double a, b;
		//iterate over the 10 values
		for (int i = 0; i<this.size();++i){
			a = this.get(i);
			b = c.get(i);
			//numerator
			sum+=a*b;
			//sum Ai & Bi squared
			nv+=a*a;
			nc+=b*b;
		}
		//sum Bi squared
		if(nc==0||nv==0)
			return 0;
		else return 1-(sum/(Math.sqrt(nv)*Math.sqrt(nc)));
	}
}
