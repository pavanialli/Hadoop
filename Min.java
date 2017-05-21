import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Min {

	public static class Mymapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable Key,Text value,Context context) throws IOException, InterruptedException
		{
			String arr[]=value.toString().split(",");

            context.write(new Text("name"), new IntWritable(Integer.parseInt(arr[2])));
			
			}
		}

	public static class Myreducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException 
		{
		int max=0;
		for(IntWritable a:value)
		{
			max=Math.max(max,a.get());
		}
		context.write(key,new IntWritable(max));
		
		}
	}

		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		     Configuration c = new Configuration();

		     Job job = Job.getInstance(c, "Minimum");
		     job.setJarByClass(Min.class);
		     job.setMapperClass(Mymapper.class);
             job.setReducerClass(Myreducer.class);
		     //job.setNumReduceTasks(7);
		     job.setMapOutputKeyClass(Text.class);
		     job.setMapOutputValueClass(IntWritable.class);
		     job.setOutputKeyClass(Text.class);
		     job.setOutputValueClass(IntWritable.class);
		     FileSystem.get(c).delete(new Path(args[1]), true);
		     FileInputFormat.addInputPath(job, new Path(args[0]));
		     FileOutputFormat.setOutputPath(job, new Path(args[1]));
		     System.exit(job.waitForCompletion(true) ? 0 : 1);

		 }

		}
