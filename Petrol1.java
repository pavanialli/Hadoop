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




public class Petrol1 {

	public static class Mymapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException
		{
			String arr[]=value.toString().split(",");
			int volumeout=Integer.parseInt(arr[5]);
		    context.write(new Text(arr[1]),new IntWritable(volumeout));
		}
	}
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key,Iterable<IntWritable>value,Context context) throws IOException, InterruptedException
		{
		int sum = 0;
	      for (IntWritable a : value) 
	      {
	         sum += a.get();
	      }
	      
	      context.write(key,new IntWritable(sum));
	   }
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration c = new Configuration ();
		Job job=Job.getInstance(c,"pet");
		job.setJarByClass(Petrol1.class);
		job.setMapperClass(Mymapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileSystem.get(c).delete(new Path(args[1]),true); /// no need to change the op1 always 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	}


	