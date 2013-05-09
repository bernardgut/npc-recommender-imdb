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

public class VFormating {
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
	
	public static class VFReducer extends Reducer<IntWritable, Text, Text, NullWritable>{

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(IntWritable arg0, Iterable<Text> arg1, Context arg2)
				throws IOException, InterruptedException {
			String line=arg0.toString()+",";
			for(Iterator<Text> it = arg1.iterator(); it.hasNext();){
				line+=it.toString().split(",")[1]+",";
			}
			arg2.write(new Text(line), null);
		}
	}
	
	public static long run(String[] args, final int REDUCERS) 
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
		FileOutputFormat.setOutputPath(job, new Path(args[0]+"/V0"));
		//return number of converged centers
		return (job.waitForCompletion(true) ? 0 : -1);
	}
}
