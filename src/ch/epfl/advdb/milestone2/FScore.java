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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ch.epfl.advdb.milestone2.io.Fetchers;

/**
 * @author Bernard Gütermann
 *
 */
public class FScore {
	
	public static class FMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String l[] = value.toString().split(",");
			context.write(new IntWritable(Integer.valueOf(l[1])), new Text(l[0]+","+l[2]));
		}
	}
	
	public static class FReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		HashMap<Integer, HashSet<Integer>> recommanderResults; 
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			recommanderResults = Fetchers.fetchResults(context.getConfiguration()); 
			super.setup(context);
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(IntWritable arg0, Iterable<Text> arg1, Context context)
				throws IOException, InterruptedException {
			//construct user ratings set
			//TODO
			
			HashSet<Integer> resultSet = recommanderResults.get(arg0.get());
			HashSet<Integer> relevantSet = new HashSet<Integer>();

			for (Text t : arg1){
				relevantSet.add(Integer.valueOf(t.toString().split(",")[0]));
			}
			System.out.println("FScore for movie "+arg0.get()+", result size : "+resultSet.size()
					+", total relvant size : "+relevantSet.size());
			
			//intersection
			relevantSet.retainAll(resultSet);
			double n = relevantSet.size();
			//recall
			double recall = n/relevantSet.size();
			//precision
			double precision = n/resultSet.size();
			//fscore
			double fscore = 1/((1/recall)+(1/precision));
			System.out.println("\tRecall:"+recall+" ; Precision:"+precision+" ; FScore:"+fscore);
			context.write(arg0, new Text(recall+","+precision+","+fscore));
		}
	}

	public static int run(String[] args, final int REDUCERS) 
			throws IOException, ClassNotFoundException, InterruptedException{
		//set config
		Configuration c = new Configuration();
		c.set("CPATH", args[2]+"/recommander");

		Job job = new Job(c, "FScore");
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(FScore.class);
		job.setMapperClass(FMapper.class);
		job.setReducerClass(FReducer.class);
		//mapOutput,reduceOutput
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);  
		//IO
		FileInputFormat.addInputPaths(job, args[1]+"/ratings");
		FileOutputFormat.setOutputPath(job, new Path(args[2]+"/fscore"));
		
		return (job.waitForCompletion(true) ? 0 : -1);
	}
}
