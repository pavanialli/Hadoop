import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/*Find the number of movies with duration more than 1.5 hours*/


public class Hours {

	public static class Mymapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String arr[]=value.toString().split(",");
			int secs=Integer.parseInt(arr[4]);
			context.write(new Text(arr[1]),new IntWritable (secs));
		}
	}
	
	public static class Myreducer extends Reducer<Text,IntWritable,Text,DoubleWritable>
	{
		public void reduce(Text key,Iterable<IntWritable>value,Context context) throws IOException, InterruptedException
		{
		   	for(IntWritable a:value)
			{
			float hours =a.get()/3600;
			if (hours>1.5)	
		{
			context.write(key,new DoubleWritable(hours));
		}}
		
	}}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration c = new Configuration ();
		Job job=Job.getInstance(c,"hour");
		job.setJarByClass(Hours.class);
		job.setMapperClass(Mymapper.class);
		job.setReducerClass(Myreducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileSystem.get(c).delete(new Path(args[1]),true); /// no need to change the op1 always 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

	
