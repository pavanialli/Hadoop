import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Rating {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,FloatWritable>
	  {
		  public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		  {
			  String arr[]=value.toString().split(",");
			  float Rating=Float.parseFloat(arr[3]);
			  String name = arr[1];
				  context.write(new Text(name),new FloatWritable(Rating));
			  
		  }
	  }
	
	public static class Myreducer extends Reducer<Text,FloatWritable,Text,FloatWritable>
	{
		public void reduce(Text key,Iterable<FloatWritable>value,Context context) throws IOException, InterruptedException
		{
			//int c=0;
			for (FloatWritable a:value)
			{
				if(a.get()> 3.9){
			
			context.write(key,new FloatWritable(a.get()));
		}}

		
	}}
	

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
	  
		Configuration c = new Configuration ();
		Job job=Job.getInstance(c,"rating");
		job.setJarByClass(Rating.class);
		job.setMapperClass(Mymapper.class);
		job.setReducerClass(Myreducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		FileSystem.get(c).delete(new Path(args[1]),true); /// no need to change the op1 always 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

	


