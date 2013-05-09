/**
 * 
 */
package ch.epfl.advdb.milestone2.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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
			if (fileName[fileName.length-1].contains("part-")) {
				BufferedReader br=new BufferedReader(
						new InputStreamReader(fs.open(status[i].getPath())));
				String line=br.readLine();
				//for each line
				while (line != null)
				{
					String[] l = line.split(":");
					int id = Integer.valueOf(l[0]);
					if(l.length<=1){
						//the 0 cluster centroid
						if (clusterCentroids[id]==null)
							clusterCentroids[id]=new ClusterCenter(id);
					}else{
						String[] keyValues = line.split(":")[1].split(";"); //get key-values
						String kv[];
						//iterate over the key values
						for (String e : keyValues)
						{
							kv = e.split(",");
							//populate centroids
							if (clusterCentroids[id]==null)
								clusterCentroids[id]=new ClusterCenter(id);
							clusterCentroids[id].put(Integer.valueOf(kv[0]), 
									Double.parseDouble(kv[1]));
						}

					}
					line = br.readLine();
				}
				br.close();
			}
		}
		return clusterCentroids;
	}
}
