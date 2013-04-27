/**
 * 
 */
package ch.epfl.advdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import ch.epfl.advdb.io.*;

/**
 * @author Bernard Gütermann
 *
 */
public class Kmeans {
	
	public static double getDistance(FeatureVector v, ClusterCenter c){
		//TODO
		return 0;
	}
	
	/**
	 * Map : For a given feature vector, Find closest centroid 
	 * @author mint05
	 *
	 */
	public static class KmeansMapper extends Mapper<Text,FeatureVector,ClusterCenter,FeatureVector>{

		ClusterCenter[] clusterCentroids;
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration c = context.getConfiguration();
			clusterCentroids = new ClusterCenter[c.getInt("k", 0)];
			FileSystem fs = FileSystem.get(c);
			FileStatus[] status = fs.listStatus(new Path(c.get("CPATH")));  
			//For each correponding file
			for (int i=0;i<status.length;i++)
			{
				String fileName[] = status[i].getPath().toString().split("/");
				if (fileName[fileName.length-1].contains("cen-")) {
					BufferedReader br=new BufferedReader(
							new InputStreamReader(fs.open(status[i].getPath())));
					String line=br.readLine();
					//for each line
					while (line != null)
					{
						int indexOfMean = Integer.valueOf(line.split(":")[0]);
						String[] keyValues = line.split(":")[1].split(";"); //get key-values
						String kv[];
						//iterate over the key values
						for (String e : keyValues)
						{
							kv = e.split(",");
							//populate centroids
							if (clusterCentroids[indexOfMean]==null)
									clusterCentroids[indexOfMean]=new ClusterCenter(keyValues.length);
							clusterCentroids[indexOfMean].put(Integer.valueOf(kv[0]), 
									Float.parseFloat(kv[1]));
						}
						line = br.readLine();
					}
					br.close();
				}
			}
			super.setup(context);
		}
		

		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(Text key, FeatureVector value, Context context)
				throws IOException, InterruptedException {
			ClusterCenter nearest = null;
			double nearestDistance = Double.MAX_VALUE;
			double distance = 0;
			for (ClusterCenter c : clusterCentroids)
			{
				distance = getDistance(value, c);
				if(nearest==null)
				{
					nearest=c;
					nearestDistance=distance;
				}
				else
				{
					if (distance < nearestDistance)
					{
						nearest = c;
						nearestDistance=distance;
					}
				}
			}
			context.write(nearest, value);
		}
	}
	
	public static class KmeansReducer extends Reducer<ClusterCenter,FeatureVector,Text,Text>{

		@Override
		protected void reduce(ClusterCenter key, Iterable<FeatureVector> values, Context context)
				throws IOException, InterruptedException {
			

		}
		
	}
	
	public int run(String[] args, int iteration){
		Configuration conf = new Configuration();
		//Save params
		conf.set("CPATH", args[1]+"/"+"centroids"+iteration);
		return 0;
	}
}
