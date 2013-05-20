/*
 * BERNARD GUTERMANN (c) 2013
 */
package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ch.epfl.advdb.milestone2.counters.GLOBAL_COUNTERS;
import ch.epfl.advdb.milestone2.io.ClusterCenter;
import ch.epfl.advdb.milestone2.io.FVectorIMDB;
import ch.epfl.advdb.milestone2.io.Fetchers;

/**
 * This Class contains the hadoop task for creating the recommandation for an input 
 * training set of movies
 * @author Bernard Gütermann
 *
 */
public class Recommander {
	
	/**
	 * MAP :
	 * During the setup phase, both the K centroids of IMDB and Netflix datasets, along with their 
	 * associated movies clusters first iteration) are stored locally at each map worker so that it 
	 * is accessible throughout the map tasks. Furthermore, the output mappings of the Mapping phase 
	 * are also loaded into the local memory. 
	 * Input file is the movie features of the IMDB test, set. For a given feature vector of a 
	 * new movie, the map task computes the closest centroid in the IMDB cluster centroids set. 
	 * Once the closest centroid has been determined, the map tasks uses the previously established 
	 * 1:1 mapping to determine the corresponding Netflix cluster centroid that is the most similar 
	 * in the Netflix dataset. 
	 * The map task output the movieID as key, and the associated Netflix cluster centroid 
	 * (centroid in V) as value.
	 * @author Bernard Gütermann
	 *
	 */
	public static class RMapper extends Mapper<LongWritable, Text, IntWritable,ClusterCenter>{

		ClusterCenter[] clusterCentroidsIMDB;
		ClusterCenter[] clusterCentroidsNetflix;
		HashMap<Integer,Integer> mappings;
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration c = context.getConfiguration();
			c.set("CPATH", c.get("args[2]")+"/clusterIMDB"+GLOBAL_COUNTERS.ITERATIONS_IMDB);
			clusterCentroidsIMDB = Fetchers.fetchCenters(c);
			c.set("CPATH", c.get("args[2]")+"/clusterNetflix"+GLOBAL_COUNTERS.ITERATIONS_NETFLIX);
			clusterCentroidsNetflix = Fetchers.fetchCenters(c);
			c.set("CPATH", c.get("args[2]")+"/mapping");
			mappings = Fetchers.fetchMappings(c); 
			super.setup(context);
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			FVectorIMDB v =new FVectorIMDB(value);
			ClusterCenter nearest = null;
			float nearestDistance = Float.MAX_VALUE;
			float distance = 0;
			for (ClusterCenter c : clusterCentroidsIMDB){
				distance = v.getDistance(c);
				if(nearest==null){
					nearest=c;
					nearestDistance=distance;
				}
				else{
					if (distance < nearestDistance){
						nearest = c;
						nearestDistance=distance;
					}
				}
			}
			//use Mapping to associate with clusterNetflix
			context.write(new IntWritable(v.getId()), 
					clusterCentroidsNetflix[mappings.get(nearest.getClusterID())]);
		}
	}
	
	/**
	 * During the setup phase, the reduce worker loads U, the output of collaborative filtering 
	 * from milestone 1, into the local memory.
	 * Input is a test movieID, along with a V cluster centroid. The reduce task computes a 
	 * recommendation for the test movie by computing the recommendation vector R=U*c, where c is the 
	 * input cluster centroid. The reduce task then trim this recommendation vector of all values 
	 * that are below the T threshold (default 0) of relevance. 
	 * The reduce task outputs the movieID, along with all users that might enjoy that movie.
	 * @author Bernad Gütermann
	 *
	 */
	public static class RReducer extends Reducer<IntWritable, ClusterCenter, IntWritable, Text>{

		private double[][] U;
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			U=Fetchers.fetchUMatrix(context.getConfiguration());
			super.setup(context);
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(IntWritable arg0, Iterable<ClusterCenter> arg1, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//there is only one clustercenter per test movieID
			ClusterCenter c = arg1.iterator().next();
			StringBuilder users= new StringBuilder();
			//compute predicted score for all users
			for (int i = 0; i<U.length; ++i){
				double score = 0 ;
				for (int j = 0 ; j< U[0].length; ++j){
					score+=c.get(j)*U[i][j];
				}
				if(score>conf.getInt("T", 0))
					users.append(i).append(",");
			}
			context.write(arg0, new Text(users.toString()));
		}
	}
	
	/**
	 * @param args input path for the train/test sets and the output
	 * @param REDUCERS the number of reducers assigned to this hadoop task
	 * @param K the number of cluster centroids used in this run of K-Means
	 * @param USERS the number of users in the database
	 * @param DIMENSIONS the number of dimensions in the V matrix
	 * @param T the threshold so that a movie is deemed relevant to a user
	 * @return 0 if success
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static int run(String[] args, final int REDUCERS, final int K, final int USERS, 
			final int DIMENSIONS, final int T) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		//Save params
		conf.set("args[2]", args[2]);
		conf.set("UPATH", args[0]+"/U");
		conf.setInt("USERS", USERS);
		conf.setInt("DIMENSIONS", DIMENSIONS);
		conf.setInt("K", K);
		conf.setInt("T", T);
		conf.set("mapred.textoutputformat.separator", ",");
		
		Job job = new Job(conf, "Recommander");
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(Recommander.class);
		job.setMapperClass(RMapper.class);
		job.setReducerClass(RReducer.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(ClusterCenter.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);  
		job.setInputFormatClass(TextInputFormat.class);
		//IO
		FileInputFormat.addInputPaths(job, args[1]+"/features");
		FileOutputFormat.setOutputPath(job, new Path(args[2]+"/recommander"));
		
		return (job.waitForCompletion(true) ? 0 : -1);
	}
}
