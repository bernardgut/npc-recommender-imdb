/*
 * BERNARD GUTERMANN (c) 2013
 */
package ch.epfl.advdb.milestone2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ch.epfl.advdb.milestone2.counters.GLOBAL_COUNTERS;
import ch.epfl.advdb.milestone2.io.ClusterCenter;
import ch.epfl.advdb.milestone2.io.Fetchers;
import ch.epfl.advdb.milestone2.io.Pair;

/**
 * This Class contains the functions associated with the Mapping between the IMDB and Netflix 
 * clusters
 * @author Bernard Gütermann
 *
 */
public class Maping {
	
	/**
	 * Retrieve the outputs of the k means algorithm for bot IMDB and Netflix and performs 
	 * a 1:1 mapping between the K IMDB and the K Netflix clusters. 
	 * @param args input path for the train/test sets and the output
	 * @param K the number of cluster centroids used in this run of K-Means
	 * @return 0 if success
	 * @throws IOException
	 */
	public static int run(String[] args, final int K) throws IOException{
		//LOAD CENTROIDS
		Configuration c = new Configuration();
		c.setInt("K", K);
		System.out.println("Fetch Clusters after IMDB iteration :"+GLOBAL_COUNTERS.ITERATIONS_IMDB);
		c.set("CPATH", args[2]+"/clusterIMDB"+GLOBAL_COUNTERS.ITERATIONS_IMDB);
		Pair<ClusterCenter,String>[] imdbClusters = Fetchers.fetchClusters(c);
		System.out.println("Fetch Clusters after Netflix iteration :"+GLOBAL_COUNTERS.ITERATIONS_NETFLIX);
		c.set("CPATH", args[2]+"/clusterNetflix"+GLOBAL_COUNTERS.ITERATIONS_NETFLIX);
		Pair<ClusterCenter,String>[] netflixClusters = Fetchers.fetchClusters(c);
		System.out.println("# IMDB;Netflix centers : "+imdbClusters.length+";"+netflixClusters.length);
		//JACCARD SIMILARITY ALGO
		ArrayList<Pair<Integer, Integer>> associations = new ArrayList<Pair<Integer, Integer>>();
		for (Pair<ClusterCenter, String> c1 : imdbClusters){
			double similarity  = 0;
			double maxSimilarity = 0;
			int maxID=-1;
			for (Pair<ClusterCenter, String> c2 : netflixClusters){
				similarity = jaccardSimilarity(c1.y, c2.y);
				if (similarity>maxSimilarity){
					maxSimilarity=similarity;
					maxID=c2.x.getClusterID();
				}
			}
			//Save mapping : <IMDB, Netflix>
			associations.add(new Pair<Integer, Integer>(c1.x.getClusterID(), maxID));
		}
		return writeToDisk(args, associations);
	}

	/**
	 * writes to disk the mappings in a sequence of <I,J> where I is a clusterID for IMDB
	 * and J for Netflix
	 * @param args input path for the train/test sets and the output
	 * @param associations the set of Pairs representing the mappings
	 * @return 0 if success
	 */
	private static int writeToDisk(
			String[] args, ArrayList<Pair<Integer, Integer>> associations) {
		Path op = new Path(args[2]+"/mapping/part0");
		Configuration c= new Configuration();
		FileSystem fs;
		try{
			fs  = FileSystem.get(c);
			BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(op,true)));
			String line;
			for(Pair<Integer, Integer> p : associations){
				line= p.x+","+p.y+"\n";
				br.write(line);
			}
			br.close();
		}
		catch (IOException e){
			System.err.println("Mapper : unable to write pairs");
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	/**
	 * Computes the jacquard similarity between two strings representing set of integers. 
	 * @param y1 input set of the form "v1,v2,v3,v4,..,vn"
	 * @param y2 input set of the form "v1,v2,v3,v4,..,vn"
	 * @return
	 */
	private static double jaccardSimilarity(String y1, String y2) {
		String[] s1 = y1.split(",");
		String[] s2 = y2.split(",");
		double n = 0;
		for (String e1 : s1){
			for (String e2 :s2){
				if (e1.equals(e2))
					n++;
			}
		}
		//|AuB|=|A|+|B|-|AnB|
		return n/(s1.length+s2.length-n);
	}
}