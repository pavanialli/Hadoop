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



public class Wordcount {

	public static class Mymapper extends Mapper<LongWritable , Text ,Text ,IntWritable>
	{
		public void map(LongWritable key, Text value,Context context ) throws IOException, InterruptedException
		{
			String a[]=value.toString().split(",");
			
			int i=1;
			for(String s:a)
			{
				context.write(new Text(s), new IntWritable(i));
				
			}
		}
	}

		public static class MyReducer extends Reducer<Text,IntWritable , Text,IntWritable>
		{
			public void reduce(Text key , Iterable<IntWritable>value,Context context) throws IOException, InterruptedException{
		   int c=0;
			
				for (IntWritable i :value)
				{
					c=c+i.get();
					
				}context.write(key,new IntWritable(c));
				
	}
		}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		
		Configuration c=new Configuration(); // location is given same as XML file
		Job job=Job.getInstance(c,"hello");//wordcount is userdefined name any name 
		job.setJarByClass(Wordcount.class);
		job.setMapperClass(Mymapper.class);
		
		job.setReducerClass(MyReducer.class);
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileSystem.get(c).delete(new Path(args[1]),true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
		}
	

		
