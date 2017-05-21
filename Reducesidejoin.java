import java.io.IOException;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Reducesidejoin {

	public static class Mymapper1 extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			
			String arr[]=value.toString().split(",");
			int id = Integer.parseInt(arr[0].toString());
			//String name = arr[];
			context.write( new Text(arr[0]),new Text(arr[1]+"\ta"));
			}
	}
	
	public static class Mymapper2 extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable Key,Text value,Context context) throws IOException, InterruptedException
		{
			String arr[]=value.toString().split(",");
			context.write(new Text (arr[1]), new Text (arr[2]+"\tb"));
		}
		
	}
	public static class MyReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> value , Context context) throws IOException, InterruptedException
		{ 
		    String gmail = null;
			String name = null;
			String marks;
			String table = " ";
			String ss[]=value.toString().split("\t");
			if(ss[1].equals("a"))
			{
				name=ss[1];
			}
			else if(ss[2].equals("b"))
			{
			marks=ss[2];
		}
		/*	else if(ss[3].equals("b"))	
			{
				gmail=ss[3];
			}*/
			table= name + "-" +gmail;
			 context.write(key, new Text(table));
		}
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
	   Configuration c = new Configuration();
        Job job = Job.getInstance(c, "sss");
        job.setJarByClass(Reducesidejoin.class);

        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        FileSystem.get(c).delete(new Path(args[2]), true);
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, Mymapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, Mymapper2.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
