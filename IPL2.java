import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;                                                               /* battle ship*/
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class IPL2
{
	public static class MyMapper extends Mapper<LongWritable,Text,IntWritable,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String arr[]=value.toString().split(",");
			int year=Integer.parseInt(arr[1]);
			context.write(new IntWritable(year),new Text(arr[10]));
		}
	}

	public static class MyReducer extends Reducer<IntWritable,Text,IntWritable,Text>
	{
	
	public void Reduce(IntWritable key,Iterable<Text>value,Context context) throws IOException, InterruptedException
		{
		int c=0;
			for(Text a:value)
			{
				c++;
				//List<String> list = new ArrayList<String>();
				
			}
			context.write(key,new Text(String.valueOf(c)));
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
	Configuration c = new Configuration ();
	Job job=Job.getInstance(c,"IPL2");
	job.setJarByClass(IPL2.class);
	job.setMapperClass(MyMapper.class);
	job.setReducerClass(MyReducer.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(Text.class);
	
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(Text.class);
	FileSystem.get(c).delete(new Path(args[1]),true); // / no need to change the op1 always 
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}




	
	
