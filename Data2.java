import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Data2 {

	public static class Mymapper extends Mapper< LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable Key , Text value , Context context) throws IOException, InterruptedException
		{
		  String arr[]=value.toString().split(",");
		
		String year=context.getConfiguration().get("lll");
		  int i = Integer.parseInt(arr[0]);
		 int lll =Integer.parseInt(year);
		
		 
		context.write( new Text("total"),new IntWritable(i+lll));
			  }
	}
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> value,Context context) throws IOException, InterruptedException
		{    
			int count=0;
			for(IntWritable K:value)
			{
				if(K.get()>18)
				{
					count++;
					
				}
			
			}
			context.write(key,new IntWritable (count));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration ();
	    int n=0;
		Scanner sc=new Scanner(System.in);
		System.out.println("Enter the number");
		 String year=sc.next();
		c.set("lll", year);
		Job job=Job.getInstance(c,"red");
		
		job.setJarByClass(Data2.class);
		job.setReducerClass(MyReducer.class);
		job.setMapperClass(Mymapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileSystem.get (c).delete(new Path(args[1]),true); /// no need to change the op1 always 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}