


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

	public class IPL3 {
    public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>
		{
			public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
			{
				int i=1;
				String arr[]=value.toString().split(",");
			    //int runs=Integer.parseInt(arr[11]);
				//int wickets=Integer.parseInt(arr[12]);
				context.write(new Text("winbyruns winbywickets"), new Text((arr[11]+","+arr[12])));
			}
		}
		
		public static class MyReducer extends Reducer<Text,Text,Text,Text>
		{
			public void reduce(Text key,Iterable<Text>value,Context context) throws IOException, InterruptedException
			{
				String str="";
				int c=0;
				int k=0;
			    for(Text a:value)
				{
			    String arr1[]=a.toString().split(",");
			    int runs=Integer.parseInt(arr1[0].toString());
			    int wickets=Integer.parseInt(arr1[1].toString());
			    
			    	if(runs > 0)
			    	{
			    		c++;
			    	}
			    	else if(wickets > 0)
			    	{
			    		k++;
			    	}
			    	str=c+" "+k;
				}
			    
			    context.write(key,new Text(str));
			}
			
		}
		public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
			Configuration c = new Configuration ();
			Job job=Job.getInstance(c,"IPL");
			job.setJarByClass(IPL3.class);
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


