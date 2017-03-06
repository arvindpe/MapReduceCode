package logerror;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass
{
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	{	
		
		
		IntWritable one=new IntWritable(1);
		
		public void map(LongWritable key,Text value,Context context)
		{
			try
			{
				String [] record=value.toString().split(" ");
				String module=record[2];
				String type=record[3];
				if(type.equals("[ERROR]"))
				{
				
					context.write(new Text(module),one );
				
				
				}
				
			}
			catch(Exception ex)
			{
				System.out.println(ex.getMessage());
			}
			
			
		}
	}
	
	public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
	   {
		Text maxModule=new Text();
    	int max=0;

		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
		    {
		    	
		    	int sum = 0;
		         for (IntWritable val : values)
		         {       
		        		
		          sum+=val.get();
		         
     	         }	
		       if(sum>max)
		       {
		    	   max=sum;
		    	   maxModule.set(key);
		       }
		         
		    } 
		    protected void cleanup(Context context) throws IOException, InterruptedException 
		    	{
		    	
		    	context.write(maxModule, new IntWritable(max));
		    	
	               
	            }
		    	
		    	
		    	
	   }

		    
	   
	  public static void main(String[] args) throws Exception 
	  {
		    Configuration conf = new Configuration();
		   
		    Job job = new Job(conf, "max error log modulename");
		    
		    job.setJarByClass(MainClass.class);
		    job.setMapperClass(MapClass.class);
		    job.setReducerClass(ReduceClass.class);
		    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		   
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	  
}
