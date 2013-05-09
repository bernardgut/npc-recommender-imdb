/**
 * 
 */
package ch.epfl.advdb.milestone2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
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
	 * Map : For a given feature vector, Find closest centroid 
	 * @author mint05
	 *
	 */
	public static class KmeansMapper extends Mapper<LongWritable, Text, ClusterCenter,FVector>{

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
			clusterCentroids = Fetchers.fetchCenters(context.getConfiguration());
			super.setup(context);
		}


		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String t = context.getConfiguration().get("TYPE");
			FVector v;
			if (t.equals("IMDB")){
				v = new FVectorIMDB(value);
			}else if (t.equals("Netflix")){
				v = null;
			}else throw new InterruptedException("Invalid TYPE specification in configuration");
			
			ClusterCenter nearest = null;
			double nearestDistance = Float.MAX_VALUE;
			double distance = 0;
			for (ClusterCenter c : clusterCentroids)
			{
				distance = v.getDistance(c);
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

	public static class KmeansReducer extends Reducer<ClusterCenter,FVector,ClusterCenter, NullWritable>{

		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(ClusterCenter key, Iterable<FVector> values, Context context)
				throws IOException, InterruptedException {
			ClusterCenter newCentre = new ClusterCenter(key.getClusterID());
			double count=0;
			for(FVector f : values){
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

	public static long runIMDB(String[] args, int iteration, final int REDUCERS, final int K) 
			throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		//Save params
		conf.set("CPATH", args[2]+"/"+"clusterIMDB"+iteration);
		conf.setInt("K", K);
		conf.set("TYPE", "IMDB");
		Job job = new Job(conf, "k-Means-IMDB"+iteration);
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(KMeans.class);
		job.setMapperClass(KmeansMapper.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(KmeansReducer.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(ClusterCenter.class);
		job.setMapOutputValueClass(FVectorIMDB.class);
		job.setOutputKeyClass(ClusterCenter.class);
		job.setOutputValueClass(NullWritable.class);  

		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(NullOutputFormat.class);
		//MultipleOutputs.addNamedOutput(job,"out", TextOutputFormat.class, Text.class, Text.class);

		FileInputFormat.addInputPaths(job, args[0]+"/features");
		FileOutputFormat.setOutputPath(job, new Path(args[2]+"/clusterIMDB"+(iteration+1)));
		//return number of converged centers
		return (job.waitForCompletion(true) ? 
				job.getCounters().findCounter(KMEANS.CONVERGED).getValue() : -1);
	}
	
	public static long runNetflix(String[] args, int iteration, final int REDUCERS, final int K) 
			throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		//Save params
		conf.set("CPATH", args[2]+"/"+"clusterNetflix"+iteration);
		conf.setInt("K", K);
		Job job = new Job(conf, "kMeans-Netflix-"+iteration);
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(KMeans.class);
		job.setMapperClass(KmeansMapper.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(KmeansReducer.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(ClusterCenter.class);
		job.setMapOutputValueClass(FVectorNetflix.class);
		job.setOutputKeyClass(ClusterCenter.class);
		job.setOutputValueClass(NullWritable.class);  

		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(NullOutputFormat.class);
		//MultipleOutputs.addNamedOutput(job,"out", TextOutputFormat.class, Text.class, Text.class);

		FileInputFormat.addInputPaths(job, args[0]+"/V0");
		FileOutputFormat.setOutputPath(job, new Path(args[2]+"/clusterNetflix"+(iteration+1)));
		//return number of converged centers
		return (job.waitForCompletion(true) ? 
				job.getCounters().findCounter(KMEANS.CONVERGED).getValue() : -1);
	}
}
