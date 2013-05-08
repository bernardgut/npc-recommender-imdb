/**
 * 
 */
package ch.epfl.advdb.milestone2;

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

import ch.epfl.advdb.milestone2.io.*;

/**
 * @author Bernard Gütermann
 *
 */
public class KMeans {

	public static enum KMEANS {
		CONVERGED
	};

	/**
	 * Distance function : uses cosine similarity between cluster centers and features
	 * @param v
	 * @param c
	 * @return
	 */
	public static double getDistance(FeatureVector v, ClusterCenter c){
		double sum=0;
		double nv = 0;
		double nc=0;
		for (int index : v){
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

	/**
	 * Map : For a given feature vector, Find closest centroid 
	 * @author mint05
	 *
	 */
	public static class KmeansMapper extends Mapper<LongWritable, Text, ClusterCenter,FeatureVector>{

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
			clusterCentroids = fetchCenters(context.getConfiguration());
			super.setup(context);
		}


		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			FeatureVector v = new FeatureVector(value);
			ClusterCenter nearest = null;
			double nearestDistance = Float.MAX_VALUE;
			double distance = 0;
			for (ClusterCenter c : clusterCentroids)
			{
				distance = getDistance(v, c);
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
			context.write(nearest, v);
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
				System.out.println("equals");
				context.getCounter(KMEANS.CONVERGED).increment(1);
			}
		}
	}

	public static long run(String[] args, int iteration, final int REDUCERS, final int K) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		//Save params
		conf.set("CPATH", args[2]+"/"+"centroids"+iteration);
		conf.setInt("K", K);
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

		FileInputFormat.addInputPaths(job, args[0]+"/features");
		FileOutputFormat.setOutputPath(job, new Path(args[2]+"/centroids"+(iteration+1)));
		//return number of converged centers
		return (job.waitForCompletion(true) ? 
				job.getCounters().findCounter(KMEANS.CONVERGED).getValue() : -1);
	}
}
