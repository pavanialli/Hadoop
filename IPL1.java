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

/*Find total number of matches palyed in stadium
*/
public class IPL1
{
	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String arr[]=value.toString().split(",");
			    
			context.write(new Text("matches & stadium"),new Text((arr[10]+","+arr[14])));
		}
	}

	public static class MyReducer extends Reducer<Text,Text,Text,Text>
	{
	
	public void Reduce(Text key,Iterable<Text>value,Context context) throws IOException, InterruptedException
		{
		String str=" ";
		int c=0;
		int k=0;
		for(Text a: value)
		{
			String arr1[]=a.toString().split(",");
			int winteam=Integer.parseInt(arr1[0].toString());
			int stadium=Integer.parseInt(arr1[14].toString());
			if(winteam>0)
			{
				c++;
			}
			else if(stadium>0)
			{
				k++;
			}
			str=c+" "+k;
		}
		context.write(key,new Text(str));
		}}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
	Configuration c = new Configuration ();
	Job job=Job.getInstance(c,"IPL1");
	job.setJarByClass(IPL1.class);
	job.setMapperClass(MyMapper.class);
	job.setReducerClass(MyReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	FileSystem.get(c).delete(new Path(args[1]),true); // / no need to change the op1 always 
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}




	
	
