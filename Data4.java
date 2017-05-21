import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Data4 {

	public static class MyMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String arr[]=value.toString().split(",");
			int i = Integer.parseInt(arr[0]);
			if ((i >19)&&(i<26))
			context.write(new IntWritable(i),new IntWritable(i) );
		}
	}
	
	public static class MyPartitioner extends Partitioner<IntWritable,IntWritable>
	{

		public int getPartition(IntWritable key, IntWritable value, int repo) {
			if( key.get() ==20)
			{
			return 0;
		}
			if(key.get()==21)
			{
				return 1;
			}
			if(key.get()==22)
			{
				return 2;
		    }
			if(key.get()==23)
			{
				return 3;
			}
		  if(key.get()==24)
		  {
			  return 4;
			  
		  }
		  if(key.get()==25)
		  {
			  return 5;
			  
		  }
		  else
		  {
			  return 6;
		  }
	}
	}
		
		public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
		{
			public void reducer(IntWritable key,Iterable<IntWritable> value,Context context ) throws IOException, InterruptedException
			{
			int count=0;
				for(IntWritable a:value)
				{
					count++;
				}
				context.write(key,new IntWritable(count));
			}
			
			
		}
		 public static void main(String[] args) throws IOException,
         ClassNotFoundException, InterruptedException {
     Configuration c = new Configuration();

     Job job = Job.getInstance(c, "educated from native and foreign");
     job.setJarByClass(Data4.class);
     job.setMapperClass(MyMapper.class);

     job.setReducerClass(MyReducer.class);
     job.setPartitionerClass(MyPartitioner.class);
     job.setNumReduceTasks(7);
     job.setMapOutputKeyClass(IntWritable.class);
     job.setMapOutputValueClass(IntWritable.class);
     job.setOutputKeyClass(IntWritable.class);
     job.setOutputValueClass(IntWritable.class);
     FileSystem.get(c).delete(new Path(args[1]), true);
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));
     System.exit(job.waitForCompletion(true) ? 0 : 1);

 }

}
