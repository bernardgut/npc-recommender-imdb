/*
 * BERNARD GUTERMANN (c) 2013
 */
package ch.epfl.advdb.milestone2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ch.epfl.advdb.milestone2.counters.GLOBAL_COUNTERS;
import ch.epfl.advdb.milestone2.counters.KMEANS_COUNTERS;
import ch.epfl.advdb.milestone2.io.ClusterCenter;
import ch.epfl.advdb.milestone2.io.FVector;
import ch.epfl.advdb.milestone2.io.FVectorIMDB;
import ch.epfl.advdb.milestone2.io.FVectorNetflix;
import ch.epfl.advdb.milestone2.io.Fetchers;

/** Class containing the Hadoop K Means Algorithm for both IMDB and Netflix datasets.
 * @author Bernard Gütermann
 */
public class KMeans{

	/**
	 * Map : 
	 * During the setup phase, the set of cluster centroids of previous iteration 
	 * (or seeds if it is the first iteration) is stored locally at each map worker so that
	 *  it is accessible throughout the map tasks. 
	 *  Input file is the movie features of Netflix, resp. IMDB. This input does not change 
	 *  throughout the iterations. The map task reads one feature vector. For this feature vector, 
	 *  it then compares sequentially to all K cluster centroids stored in the local memory, 
	 *  and emits the closest centroid as key, with the feature vector as value. 
	 * @author Bernard Gütermann
	 *
	 */
	public static class KmeansMapper extends Mapper<LongWritable, Text, ClusterCenter,FVector>{

		ClusterCenter[] clusterCentroids;

		/**
		 * Load the set of cluster centroids from previous iteration into the local memory
		 */
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
				v = new FVectorNetflix(value);
			}else throw new InterruptedException("Invalid TYPE specification in configuration");

			ClusterCenter nearest = null;
			float nearestDistance = Float.MAX_VALUE;
			float distance = 0;
			for (int i = 0; i<context.getConfiguration().getInt("K", 0);++i){
				if(clusterCentroids[i]==null)
					throw new InterruptedException("MAP "+v.getId()+": Setup didnt load properly :"+i);
				distance = v.getDistance(clusterCentroids[i]);
				if(nearest==null){
					nearest=clusterCentroids[i];
					nearestDistance=distance;
				}
				else{
					if (distance < nearestDistance){
						nearest = clusterCentroids[i];
						nearestDistance=distance;
					}
				}
			}
			context.write(nearest, v);
		}
	}

	/**
	 * Reduce :
	 * Input is a cluster centroid along with all feature vectors that had this centroid as the closest. 
	 * The reduce task then computes the new centroid for the feature vectors input. 
	 * If the new centroid is the same as the input, we say that the centroid has converged. 
	 * If this is the case, the reduce task increment a CONVERGED enum that all reduce tasks can access.
	 * @author Bernard Gütermann
	 */
	public static class KmeansReducer extends Reducer<ClusterCenter,FVector,ClusterCenter, Text>{

		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(ClusterCenter key, Iterable<FVector> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder movieIds=new StringBuilder();
			int id = key.getClusterID();


			ClusterCenter newCentre = new ClusterCenter(id);
			float count=0;
			for(FVector f : values){
				newCentre.add(f);
				count++;
				movieIds.append(f.getId()).append(",");
				//TODO prevent deserialisation issue of FVector  (Hadoop 0.21 - unstable)
				f.clear();//https://issues.apache.org/jira/browse/HADOOP-5454
			}
			newCentre.divide(count);
			context.write(newCentre, new Text(movieIds.toString()));
			//Test for convergence criteria
			if (key.equals(newCentre)){
				context.getCounter(KMEANS_COUNTERS.CONVERGED).increment(1);
			}

		}
	}

	/**
	 * Run a Hadoop task for the K-Means algorithm for the IMDB dataset
	 * @param args input path for the train/test sets and the output
	 * @param iteration the current iteration
	 * @param REDUCERS the number of reducers assigned to this hadoop task
	 * @param K the number of cluster centroids used in this run of K-Means
	 * @return the number of converged centroids after this iteration, or -1 if failure
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static long runIMDB(String[] args, int iteration, final int REDUCERS, final int K) 
			throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		//Save params
		conf.set("CPATH", args[2]+"/clusterIMDB"+iteration);
		conf.setInt("K", K);
		conf.set("TYPE", "IMDB");
		conf.set("mapred.textoutputformat.separator", "!");
		conf.set("key.value.separator.in.input.line", "!");

		Job job = new Job(conf, "k-Means-IMDB"+iteration);
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(KMeans.class);
		job.setMapperClass(KmeansMapper.class);
		job.setReducerClass(KmeansReducer.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(ClusterCenter.class);
		job.setMapOutputValueClass(FVectorIMDB.class);
		job.setOutputKeyClass(ClusterCenter.class);
		job.setOutputValueClass(Text.class);  
		job.setInputFormatClass(TextInputFormat.class);
		//IO
		FileInputFormat.addInputPaths(job, args[0]+"/features");
		FileOutputFormat.setOutputPath(job, new Path(args[2]+"/clusterIMDB"+(iteration+1)));
		//save the number of iterations
		GLOBAL_COUNTERS.ITERATIONS_IMDB++;
		//return number of converged centers
		return (job.waitForCompletion(true) ? job.getCounters().findCounter(KMEANS_COUNTERS.CONVERGED).getValue() : -1);
	}

	/**
	 * Run a Hadoop task for the K-Means algorithm for the Netflix dataset
	 * @param args input path for the train/test sets and the output
	 * @param iteration the current iteration
	 * @param REDUCERS the number of reducers assigned to this hadoop task
	 * @param K the number of cluster centroids used in this run of K-Means
	 * @return the number of converged centroids after this iteration, or -1 if failure
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static long runNetflix(String[] args, int iteration, final int REDUCERS, final int K) 
			throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		//Save params
		conf.set("CPATH", args[2]+"/clusterNetflix"+iteration);
		conf.setInt("K", K);
		conf.set("TYPE", "Netflix");
		conf.set("mapred.textoutputformat.separator", "!");
		conf.set("key.value.separator.in.input.line", "!");

		Job job = new Job(conf, "kMeans-Netflix-"+iteration);
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(KMeans.class);
		job.setMapperClass(KmeansMapper.class);
		job.setReducerClass(KmeansReducer.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(ClusterCenter.class);
		job.setMapOutputValueClass(FVectorNetflix.class);
		job.setOutputKeyClass(ClusterCenter.class);
		job.setOutputValueClass(Text.class);  
		job.setInputFormatClass(TextInputFormat.class);
		//IO
		FileInputFormat.addInputPaths(job, args[2]+"/V0");
		FileOutputFormat.setOutputPath(job, new Path(args[2]+"/clusterNetflix"+(iteration+1)));
		//save the number of iterations
		GLOBAL_COUNTERS.ITERATIONS_NETFLIX++;
		//return number of converged centers
		return (job.waitForCompletion(true) ? job.getCounters().findCounter(KMEANS_COUNTERS.CONVERGED).getValue() : -1);
	}
}
