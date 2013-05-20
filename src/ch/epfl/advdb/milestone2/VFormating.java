/*
 * BERNARD GUTERMANN (c) 2013
 */
package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This Class contains the Hadoop task that preprocess the V Matrix of the Netflix dataset so that 
 * it is usable with the KMeans.class implementation of K means.
 * @author Bernard Gütermann
 *
 */
public class VFormating {
	
	/**
	 * MAP : 
	 * Input file is V, in the form of a sequence file of <V,d,m,v> where d\ in [0,10] is 
	 * the dimension, m is the movieID and v is the associated value. The map task reads one 
	 * line of V input and emits the movieID as key the tuple with the <userid, rating> as value.
	 * @author Bernard Gütermann
	 */
	public static class VFMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			if(line[0].equals("V")){
				context.write(new IntWritable(Integer.valueOf(line[2])), new Text(line[1]+","+line[3]));
			}
		}
	}
	
	/**
	 * REDUCE : 
	 * Input is a movieID along with all it's associated ratings in the V matrix, 
	 * Output is a sequence file comprising of lines where the ratings are ordered 
	 * according to the dimension order: <m, v1, v2, ... , v10>.
	 * @author Bernard Gütermann
	 */
	public static class VFReducer extends Reducer<IntWritable, Text, Text, NullWritable>{

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(IntWritable arg0, Iterable<Text> arg1, Context arg2)
				throws IOException, InterruptedException {
			StringBuilder line=new StringBuilder(arg0.toString()+",");
			Iterator<Text> it = arg1.iterator();
			String[] v = new String[10];
			String[] kv;
			while(it.hasNext()){
				kv=it.next().toString().split(",");
				v[Integer.valueOf(kv[0])-1]=kv[1];
			}
			for(int i = 0; i<v.length;++i)
				line.append(v[i]).append(",");
			arg2.write(new Text(line.toString()), null);
		}
	}
	
	/**
	 * Run a Hadoop task for the K-Means algorithm for the VFormating of the V matrix output
	 * of collaborative filtering. The output is stored in <output>/V0
	 * @param args input path for the train/test sets and the output
	 * @param REDUCERS the number of reducers assigned to this hadoop task
	 * @return 0 if success
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static int run(String[] args, final int REDUCERS) 
			throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		//Save params
		Job job = new Job(conf, "V-Formatting");
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(VFormating.class);
		job.setMapperClass(VFMapper.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(VFReducer.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);  

		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.addInputPaths(job, args[0]+"/V");
		FileOutputFormat.setOutputPath(job, new Path(args[2]+"/V0"));
		//return number of converged centers
		return (job.waitForCompletion(true) ? 0 : -1);
	}
}
