/**
 * 
 */
package ch.epfl.advdb.milestone2.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Bernard Gütermann
 *
 */
public class Fetchers {


	public static ClusterCenter[] fetchCenters(Configuration c) throws IOException {
		ClusterCenter[] clusterCentroids = new ClusterCenter[c.getInt("K", 0)];
		FileSystem fs = FileSystem.get(c);
		FileStatus[] status = fs.listStatus(new Path(c.get("CPATH")));  
		//For each correponding file
		assert (status.length!=0) : "fetchCenters : Invalid CPATH";
		for (int i=0;i<status.length;i++)
		{
			String fileName[] = status[i].getPath().toString().split("/");
			if (fileName[fileName.length-1].contains("part")) {
				BufferedReader br=new BufferedReader(
						new InputStreamReader(fs.open(status[i].getPath())));
				String line=br.readLine();
				//for each line
				while (line != null)
				{
					String[] l = line.split("!")[0].split(":");
					int id = Integer.valueOf(l[0]);
					if(l.length<=1){
						//the 0 cluster centroid
						if (clusterCentroids[id]==null)
							clusterCentroids[id]=new ClusterCenter(id);
					}else{
						String[] keyValues = l[1].split(";"); //get key-values
						String kv[];
						//iterate over the key values
						for (String e : keyValues)
						{
							kv = e.split(",");
							//populate centroids
							if (clusterCentroids[id]==null)
								clusterCentroids[id]=new ClusterCenter(id);
							clusterCentroids[id].put(Integer.valueOf(kv[0]), 
									Float.parseFloat(kv[1]));
						}
					}
					line = br.readLine();
				}
				br.close();
			}
		}
		return clusterCentroids;
	}

	public static Pair<ClusterCenter, String>[] fetchClusters(Configuration c) throws IOException {
		Pair<ClusterCenter, String>[] clusters = new  Pair[c.getInt("K", 0)];
		FileSystem fs = FileSystem.get(c);
		FileStatus[] status = fs.listStatus(new Path(c.get("CPATH")));  
		//For each correponding file
		assert (status.length!=0) : "fetchCenters : Invalid CPATH";
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
	
	public static HashMap<Integer, Integer> fetchMappings(Configuration c) throws IOException {
		HashMap<Integer, Integer> out = new HashMap<Integer,Integer>();
		FileSystem fs = FileSystem.get(c);
		FileStatus[] status = fs.listStatus(new Path(c.get("CPATH")));  
		//For each correponding file
		assert (status.length!=0) : "fetchMappings : Invalid CPATH";
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
	
	public static double[][] fetchUMatrix(Configuration c) throws IOException{
		double[][] matrix = new double[Integer.valueOf(c.get("USERS"))][Integer.valueOf(c.get("DIMENSIONS"))];
		FileSystem fs = FileSystem.get(c);
		FileStatus[] status = fs.listStatus(new Path(c.get("UPATH")));  
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
}
