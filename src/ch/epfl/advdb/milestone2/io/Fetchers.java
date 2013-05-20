/*
 * BERNARD GUTERMANN (c) 2013
 */
package ch.epfl.advdb.milestone2.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** This Class contains all necessary functions to perform IO from DFS fileSystem.
 * @author Bernard Gütermann
 *
 */
public class Fetchers {

	/**
	 * fetch an array of cluster centroids from filesystem. the array is ordered by clusterID
	 * @param c configuration
	 * @return ordered array of cluster centroids
	 * @throws IOException
	 */
	public static ClusterCenter[] fetchCenters(Configuration c) throws IOException {
		ClusterCenter[] clusterCentroids = new ClusterCenter[c.getInt("K", 0)];
		FileSystem fs = FileSystem.get(c);
		FileStatus[] status = fs.listStatus(new Path(c.get("CPATH")));  
		//For each correponding file
		if(status.length==0) throw new IOException("Invalid CPATH :"+c.get("CPATH"));
		int count = 0;
		for (int i=0;i<status.length;i++)
		{
			String fileName[] = status[i].getPath().toString().split("/");
			if (fileName[fileName.length-1].contains("part")) {
				BufferedReader br=new BufferedReader(
						new InputStreamReader(fs.open(status[i].getPath())));
				String line=br.readLine();
				while (line != null)
				{
					count++;
					String[] l = line.split("!")[0].split(":");
					int id = Integer.valueOf(l[0]);
					String[] keyValues = l[1].split(";"); //get key-values
					String kv[];
					//iterate over the key values
					clusterCentroids[id]=new ClusterCenter(id);
					for (String e : keyValues)
					{
						kv = e.split(",");
						//populate centroids
						clusterCentroids[id].put(Integer.valueOf(kv[0]), Float.parseFloat(kv[1]));
					}
					line = br.readLine();
				}
				br.close();
			}
		}
		return clusterCentroids;
	}

	/**
	 * fetch the clustercenters, along with their clusters, form disk
	 * @param c configuration
	 * @return a table containing pairs where each cluster centroid is associated with a string containing
	 * the set of movieID associated with this clusterCentroid
	 * @throws IOException
	 */
	public static Pair<ClusterCenter, String>[] fetchClusters(Configuration c) throws IOException {
		Pair<ClusterCenter, String>[] clusters = new  Pair[c.getInt("K", 0)];
		FileSystem fs = FileSystem.get(c);
		FileStatus[] status = fs.listStatus(new Path(c.get("CPATH")));  
		//For each correponding file
		if (status.length==0) throw new IOException("Invalid CPATH :"+c.get("CPATH"));
		for (int i=0;i<status.length;i++)
		{
			String fileName[] = status[i].getPath().toString().split("/");
			if (fileName[fileName.length-1].contains("part")) {
				BufferedReader br=new BufferedReader(
						new InputStreamReader(fs.open(status[i].getPath())));
				String line=br.readLine();
				while (line != null)
				{
					//System.out.println(line);
					String[]lr =  line.split("!");
					//LEFT
					String[] l = lr[0].split(":");
					int id = Integer.valueOf(l[0]);
					ClusterCenter cs = new ClusterCenter(id);

					String[] keyValues = l[1].split(";"); //get key-values
					String kv[];
					//iterate over the key values
					for (String e : keyValues)
					{
						kv = e.split(",");
						//populate centroids
						cs = new ClusterCenter(id);
						cs.put(Integer.valueOf(kv[0]), Float.parseFloat(kv[1]));
					}

					//CONSTRUCT PAIR
					clusters[id]=new Pair<ClusterCenter, String>(cs,lr[1]);
					line = br.readLine();
				}
				br.close();
			}
		}
		return clusters;
	}

	/**
	 * fetch the mappings between clusters from disk
	 * @param c configuration
	 * @return a HashMap where keys are IMDB clusterID and values are Netflix clusterID
	 * @throws IOException
	 */
	public static HashMap<Integer, Integer> fetchMappings(Configuration c) throws IOException {
		HashMap<Integer, Integer> out = new HashMap<Integer,Integer>();
		FileSystem fs = FileSystem.get(c);
		FileStatus[] status = fs.listStatus(new Path(c.get("CPATH")));  
		//For each correponding file
		if (status.length==0) throw new IOException("Invalid CPATH :"+c.get("CPATH"));
		for (int i=0;i<status.length;i++)
		{
			String fileName[] = status[i].getPath().toString().split("/");
			if (fileName[fileName.length-1].contains("part")) {
				BufferedReader br=new BufferedReader(
						new InputStreamReader(fs.open(status[i].getPath())));
				String line=br.readLine();
				while (line != null)
				{
					String l[] = line.split(",");
					out.put(Integer.valueOf(l[0]), Integer.valueOf(l[1]));
					line = br.readLine();
				}
				br.close();
			}
		}
		return out;
	}

	/**
	 * fetch the U matrix from disk
	 * @param c
	 * @return the U matrix in a 2D array
	 * @throws IOException
	 */
	public static double[][] fetchUMatrix(Configuration c) throws IOException{
		double[][] matrix = new double[Integer.valueOf(c.get("USERS"))][Integer.valueOf(c.get("DIMENSIONS"))];
		FileSystem fs = FileSystem.get(c);
		FileStatus[] status = fs.listStatus(new Path(c.get("UPATH")));  
		if (status.length==0) throw new IOException("Invalid UPATH :"+c.get("UPATH"));
		//For all files
		for (int i=0;i<status.length;i++){
			String fileName[] = status[i].getPath().toString().split("/");
			if (fileName[fileName.length-1].contains("part")) {
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line;
				line=br.readLine();
				while (line != null){
					String values[] = line.split(","); //get values
					//populate U
					if(values[0].equals("U"))
						matrix[Integer.valueOf(values[1])-1][Integer.valueOf(values[2])-1] = Double.parseDouble(values[3]);
					line = br.readLine();
				}
				br.close();
			}
		}
		//fs.close();
		return matrix;
	}

	/**
	 * fetch the results from the Recommander
	 * NOT USED IN THE FINAL VERSION
	 * @param c
	 * @return for each test movieID, the set of userID that might enjoy that movie
	 * @throws IOException
	 */
	public static HashMap<Integer, HashSet<Integer>> fetchResults(
			Configuration c) throws IOException {
		HashMap<Integer, HashSet<Integer>>  out = new HashMap<Integer, HashSet<Integer>> ();
		FileSystem fs = FileSystem.get(c);
		FileStatus[] status = fs.listStatus(new Path(c.get("CPATH")));  
		//For each correponding file
		if (status.length==0) throw new IOException("Invalid CPATH :"+c.get("CPATH"));
		for (int i=0;i<status.length;i++)
		{
			String fileName[] = status[i].getPath().toString().split("/");
			if (fileName[fileName.length-1].contains("part")) {
				BufferedReader br=new BufferedReader(
						new InputStreamReader(fs.open(status[i].getPath())));
				String line=br.readLine();
				while (line != null)
				{
					String l[] = line.split(",");
					HashSet<Integer> u = new HashSet<Integer>();
					for (int j = 1; j< l.length; ++j){
						u.add(Integer.valueOf(l[j]));
					}
					out.put(Integer.valueOf(l[0]), u);
					line = br.readLine();
				}
				br.close();
			}
		}
		return out;
	}

	/**
	 * compute the mean FScore from each test movie FScore
	 * NOT USER IN FINAL VERSION
	 * @param args
	 * @return the average FSCORE 
	 */
	public static double fetchFScoreMean(String[] args){
		double mean = 0;
		double count = 0;
		Path ip=new Path(args[2]+"/fscore");
		Configuration c= new Configuration();
		FileSystem fs;
		FileStatus[] status;
		try {
			fs  = FileSystem.get(c);
			status = fs.listStatus(ip);
			//For each correponding file
			for (int i=0;i<status.length;i++)
			{
				String fileName[] = status[i].getPath().toString().split("/");
				if (fileName[fileName.length-1].contains("features")||
						fileName[fileName.length-1].contains("part")) {
					BufferedReader br=new BufferedReader(
							new InputStreamReader(fs.open(status[i].getPath())));
					String line=br.readLine();
					//for each line
					while (line != null)
					{
						String[] w = line.split(",");
						mean+=Double.valueOf(w[w.length-1]);
						count++;
						line = br.readLine();
					}
					br.close();
				}
			}
			return mean/count;
		} catch (IOException e) {
			System.err.println("KSeeds : unable to fetch features");
			e.printStackTrace();
			return -1;
		}
	}
}
