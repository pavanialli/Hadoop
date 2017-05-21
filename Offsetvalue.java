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


public class Offsetvalue {

	public static class Mymapper extends Mapper<LongWritable,Text,LongWritable,Text>
	{
		public void map(LongWritable Key,Text value,Context context) throws IOException, InterruptedException
		{
			String arr[]=value.toString().split(",");
		
			context.write(Key,new Text (arr[0]));
			
		}
	}
	/*public static class Myreducer extends Reducer<LongWritable,Text,Text,LongWritable>
	{
		public void Reduce(LongWritable key ,Iterable<LongWritable>value , Context context) throws IOException, InterruptedException
		{
		
		
		}

		
	}*/
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration c=new Configuration();
			Job job=Job.getInstance(c,"hadoop");
			job.setJarByClass(Offsetvalue.class);
			job.setMapperClass(Mymapper.class);
			//job.setReducerClass(Myreducer.class);
			//job.setOutputKeyClass(LongWritable.class);
			//job.setOutputValueClass(LongWritable.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			FileSystem.get(c).delete(new Path(args[1]),true); /// no need to change the op1 always 
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

}
