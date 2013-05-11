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

import ch.epfl.advdb.milestone2.io.ClusterCenter;
import ch.epfl.advdb.milestone2.io.FVectorIMDB;
import ch.epfl.advdb.milestone2.io.Fetchers;

public class Recommander {
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
			c.set("CPATH", c.get("args[2]")+"/clusterIMDB"+Counters.ITERATIONS_IMDB);
			clusterCentroidsIMDB = Fetchers.fetchCenters(c);
			c.set("CPATH", c.get("args[2]")+"/clusterNetflix"+Counters.ITERATIONS_NETFLIX);
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
//			ArrayList<Integer> users = new ArrayList<Integer>();
			String users="";
			//compute predicted score for all users
			for (int i = 0; i<U.length; ++i){
				double score = 0 ;
				for (int j = 0 ; j< U[0].length; ++j){
					score+=c.get(j)*U[i][j];
				}
				if(score>=conf.getInt("T", 0))
//					users.add(i);
					users+=i+",";
			}
			context.write(arg0, new Text(users));
		}
	}
	
	public static int run(String[] args, final int REDUCERS, final int K, final int USERS, 
			final int DIMENSIONS) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		//Save params
		conf.set("args[2]", args[2]);
		conf.set("UPATH", args[0]+"/U");
		conf.setInt("USERS", USERS);
		conf.setInt("DIMENSIONS", DIMENSIONS);
		conf.setInt("K", K);
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
		//return number of converged centers
		return (job.waitForCompletion(true) ? 0 : -1);
	}
}
