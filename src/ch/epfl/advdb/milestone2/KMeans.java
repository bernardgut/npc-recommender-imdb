/**
 * 
 */
package ch.epfl.advdb.milestone2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ch.epfl.advdb.milestone2.io.ClusterCenter;
import ch.epfl.advdb.milestone2.io.FVector;
import ch.epfl.advdb.milestone2.io.FVectorIMDB;
import ch.epfl.advdb.milestone2.io.FVectorNetflix;
import ch.epfl.advdb.milestone2.io.Fetchers;

/**
 * @author Bernard Gütermann
 *
 */
public class KMeans{
	
	/**
	 * Map : For a given feature vector, Find closest centroid 
	 * @author mint05
	 *
	 */
	public static class KmeansMapper extends Mapper<LongWritable, Text, ClusterCenter,FVector>{

		ClusterCenter[] clusterCentroids;

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
			for (ClusterCenter c : clusterCentroids){
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
			context.write(nearest, v);
		}
	}

	public static class KmeansReducer extends Reducer<ClusterCenter,FVector,ClusterCenter, Text>{

		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(ClusterCenter key, Iterable<FVector> values, Context context)
				throws IOException, InterruptedException {
			ClusterCenter newCentre = new ClusterCenter(key.getClusterID());
			float count=0;
			String movieIds="";
			for(FVector f : values){
				newCentre.add(f);
				count++;
				movieIds+=f.getId()+",";
				//TODO prevent deserialisation issue of FVector  (Hadoop 0.21 - unstable)
				f.clear();//https://issues.apache.org/jira/browse/HADOOP-5454
			}
			newCentre.divide(count);
			context.write(newCentre, new Text(movieIds));
			//Test for convergence criteria
			if (key.equals(newCentre)){
				context.getCounter(KMEANS.CONVERGED).increment(1);
			}
		}
	}

	public static int runIMDB(String[] args, int iteration, final int REDUCERS, final int K) 
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
		Counters.ITERATIONS_IMDB++;
		//return number of converged centers
		return (job.waitForCompletion(true) ? 
				(int)job.getCounters().findCounter(KMEANS.CONVERGED).getValue() : -1);
	}
	
	public static int runNetflix(String[] args, int iteration, final int REDUCERS, final int K) 
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
		FileInputFormat.addInputPaths(job, args[0]+"/V0");
		FileOutputFormat.setOutputPath(job, new Path(args[2]+"/clusterNetflix"+(iteration+1)));
		//save the number of iterations
		Counters.ITERATIONS_NETFLIX++;
		//return number of converged centers
		return (job.waitForCompletion(true) ? 
				(int)job.getCounters().findCounter(KMEANS.CONVERGED).getValue() : -1);
	}
}
