/*
 * BERNARD GUTERMANN (c) 2013
 */
package ch.epfl.advdb.milestone2.io;

/**This Class represents a tuple of two elementss
 * @author Bernard Gütermann
 *
 */
public class Pair<T1,T2> {

	public final T1 x; 
	public final T2 y; 
	
	/**
	 * Creates a tuple of length 2 with Type T1 for x and T2 for y
	 * @param x
	 * @param y
	 */
	public Pair(T1 x, T2 y) {
		this.x=x;
		this.y=y;
	}
}
