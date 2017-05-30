import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

/*Count the total number of movies in the list
*/

public class Moviecount {

	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String arr[]=value.toString().split(",");
			//int id=Integer.parseInt(arr[0]);
			context.write(new Text("total movies"),new Text((arr[1])));
		}
	}
	
	public static class MyReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text>value,Context context) throws IOException, InterruptedException
		{
		 int c=0;
	
			for(Text a:value)
			{
			c++;
			
			}
			context.write(key, new Text(String.valueOf(c)));
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration ();
		Job job=Job.getInstance(c,"movie");
		job.setJarByClass(Moviecount.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem.get(c).delete(new Path(args[1]),true); /// no need to change the op1 always 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	}


	
