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




public class Agrgate1 {

	public static class Mymapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException
		{
		String arr[]=value.toString().split(",");
		context.write(new Text(arr[1]), new IntWritable(Integer.parseInt(arr[2])));
		}
	}
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reducer(Text key , Iterable<IntWritable> value , Context context) throws IOException, InterruptedException
		{  
			int c=0;
			
			for(IntWritable i:value)
			{
				c=c+i.get();
				//String[] arr = null;
			
				//context.write(new Text(arr[1]), new IntWritable(c));
			}
			
			context.write(key, new IntWritable(c));
		}
		
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException  {
		

		Configuration c=new Configuration(); // location is given same as XML file
		Job job=Job.getInstance(c,"pav");//wordcount is userdefined name any name 
		job.setJarByClass(Agrgate1.class);
		job.setMapperClass(Mymapper.class);
		
		job.setReducerClass(MyReducer.class);
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileSystem.get(c).delete(new Path(args[1]),true); /// no need to change the op1 always 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
		}
	