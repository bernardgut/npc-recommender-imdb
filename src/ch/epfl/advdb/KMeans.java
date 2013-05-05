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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ch.epfl.advdb.io.*;

/**
 * @author Bernard Gütermann
 *
 */
public class KMeans {
	
	public static enum KMEANS {
		  CONVERGED
		};
	
	/**
	 * Distance function : uses cosine distance between cluster centers and features
	 * @param v
	 * @param c
	 * @return
	 */
	public static float getDistance(FeatureVector v, ClusterCenter c){
		float sum=0;
		float nv = 0;
		float nc=0;
		for (int index : v){
			if(c.get(index)!=null){
				sum+=c.get(index);
				nc+=c.get(index);
			}
			nv++;
		}
		if(nc==0||nv==0)
			return 0;
		else return sum/(nv*nc);
	}
	
	public static ClusterCenter[] fetchCenters(Configuration c) throws IOException {
		ClusterCenter[] clusterCentroids = new ClusterCenter[c.getInt("k", 0)];
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
					int id = Integer.valueOf(line.split(":")[0]);
					String[] keyValues = line.split(":")[1].split(";"); //get key-values
					String kv[];
					//iterate over the key values
					for (String e : keyValues)
					{
						kv = e.split(",");
						//populate centroids
						if (clusterCentroids[id]==null)
								clusterCentroids[id]=new ClusterCenter(keyValues.length);
						clusterCentroids[id].put(Integer.valueOf(kv[0]), 
								Float.parseFloat(kv[1]));
					}
					line = br.readLine();
				}
				br.close();
			}
		}
		return clusterCentroids;
	}
	
	
	public long run(String[] args, int iteration, final int REDUCERS) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		//Save params
		conf.set("CPATH", args[1]+"/"+"centroids"+iteration);
		
		Job job = new Job(conf, "k-means-"+iteration);
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(KMeans.class);
		job.setMapperClass(KmeansMapper.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(KmeansReducer.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(ClusterCenter.class);
		job.setMapOutputValueClass(FeatureVector.class);
		job.setOutputKeyClass(ClusterCenter.class);
		job.setOutputValueClass(NullWritable.class);  

		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(NullOutputFormat.class);
		//MultipleOutputs.addNamedOutput(job,"out", TextOutputFormat.class, Text.class, Text.class);

		FileInputFormat.addInputPaths(job, args[1]+"/train/features");
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/centers"+(iteration+1)));
		//return number of converged centers
		return (job.waitForCompletion(true) ? job.getCounters().findCounter(KMEANS.CONVERGED).getValue() : -1);
	}
	
	/**
	 * Map : For a given feature vector, Find closest centroid 
	 * @author mint05
	 *
	 */
	public static class KmeansMapper extends Mapper<IntWritable,FeatureVector,ClusterCenter,FeatureVector>{

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
			clusterCentroids = fetchCenters(context.getConfiguration());
			super.setup(context);
		}
		

		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(IntWritable key, FeatureVector value, Context context)
				throws IOException, InterruptedException {
			ClusterCenter nearest = null;
			float nearestDistance = Float.MAX_VALUE;
			float distance = 0;
			for (int i=0;i<clusterCentroids.length;++i)
			{
				distance = getDistance(value, clusterCentroids[i]);
				if(nearest==null)
				{
					nearest=clusterCentroids[i];
					nearestDistance=distance;
				}
				else
				{
					if (distance < nearestDistance)
					{
						nearest = clusterCentroids[i];
						nearestDistance=distance;
					}
				}
			}
			context.write(nearest, value);
		}
	}
	
	public static class KmeansReducer 
			extends Reducer<ClusterCenter,FeatureVector,ClusterCenter, NullWritable>{

		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(ClusterCenter key, Iterable<FeatureVector> values, Context context)
				throws IOException, InterruptedException {
			ClusterCenter newCentre = new ClusterCenter(key.getClusterID());
			double count=0;
			for(FeatureVector f : values){
				newCentre.add(f);
				count++;
			}
			newCentre.divide(count);
			context.write(newCentre, null);
			if (key.equals(newCentre)){
				context.getCounter(KMEANS.CONVERGED).increment(1);
			}
		}
	}
}
