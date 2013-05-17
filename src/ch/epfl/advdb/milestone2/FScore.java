/**
 * 
 */
package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ch.epfl.advdb.milestone2.io.Fetchers;

/**
 * @author Bernard Gütermann
 *
 */
public class FScore {
	
	public static class FMapperResults extends Mapper<LongWritable, Text, IntWritable, Text> {

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String l[] = value.toString().split(",");
			StringBuilder out = new StringBuilder().append("RES:");
			for(int i = 1; i< l.length ;++i)
				out.append(l[i]).append(",");
			context.write(new IntWritable(Integer.valueOf(l[0])), new Text(out.toString()));
		}
		
	}
	
	public static class FMapperRatings extends Mapper<LongWritable, Text, IntWritable, Text> {

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String l[] = value.toString().split(",");
			context.write(new IntWritable(Integer.valueOf(l[1])), new Text("RAT:"+l[0]+","+l[2]));
		}
	}
	
	public static class FReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

//		HashMap<Integer, HashSet<Integer>> recommanderResults; 
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
//			recommanderResults = Fetchers.fetchResults(context.getConfiguration()); 
			super.setup(context);
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(IntWritable arg0, Iterable<Text> arg1, Context context)
				throws IOException, InterruptedException {
			HashSet<Integer> resultSet = new HashSet<Integer>();
			HashSet<Integer> relevantSet = new HashSet<Integer>();
			HashSet<Integer> userSet = new HashSet<Integer>();	//set of users for which there is a rating
			//construct user ratings set
			for (Text t : arg1){
				String[] l = t.toString().split(":");
				if(l[0].equals("RES")){
					String[] users = l[1].split(","); 
					for(String id : users)
						resultSet.add(Integer.valueOf(id));
				}
				else {
					String[] users = l[1].split(",");
					int user = Integer.valueOf(users[0]);
					double score = Double.valueOf(users[1]);
					userSet.add(user);
					if(score>context.getConfiguration().getInt("T", 0))
						relevantSet.add(user);
				}
			}
			
			//keep only the users that have a rating in the original netflix set
			resultSet.retainAll(userSet);
			System.out.println("FScore for movie "+arg0.get()+", result size : "+resultSet.size()
					+", total relvant size : "+relevantSet.size());
			
			//intersection
			double relevantSize = relevantSet.size();
			relevantSet.retainAll(resultSet);
			double n = relevantSet.size();	//intersection set size
			//recall
			double recall = n/relevantSize;
			//precision
			double precision = n/resultSet.size();
			//fscore
			double fscore = 2/((1/recall)+(1/precision));
			System.out.println("\tn size "+n+" ;Recall: "+recall+" ; Precision: " +precision+";FScore: "+fscore);
			context.write(arg0, new Text(recall+","+precision+","+fscore));
		}
	}

	public static int run(String[] args, final int REDUCERS, final int T) 
			throws IOException, ClassNotFoundException, InterruptedException{
		//set config
		Configuration c = new Configuration();
		c.set("CPATH", args[2]+"/recommander");
		c.setInt("T", T);
		
		Job job = new Job(c, "FScore");
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(FScore.class);
//		job.setMapperClass(FMapper.class);
		job.setReducerClass(FReducer.class);
		//mapOutput,reduceOutput
//		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);  
		//IO
//		FileInputFormat.addInputPaths(job, args[1]+"/ratings");
		MultipleInputs.addInputPath(job, new Path(args[1]+"/ratings"), 
				TextInputFormat.class, FMapperRatings.class);
		MultipleInputs.addInputPath(job, new Path(args[2]+"/recommander"), 
				TextInputFormat.class, FMapperResults.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]+"/fscore"));
		
		return (job.waitForCompletion(true) ? 0 : -1);
	}
}
