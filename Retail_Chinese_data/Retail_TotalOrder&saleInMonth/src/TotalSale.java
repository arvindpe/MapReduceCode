
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TotalSale
{
	
	
	public static class MapClass extends Mapper<LongWritable,Text,NullWritable,LongWritable>
	   {
	      public void map(LongWritable key, Text value, Context context) 
	    		  throws IOException, InterruptedException
	      
	      {	    	  
	         try
	         {
	        	 String str[]=value.toString().split(";");
	 			
				 context.write(NullWritable.get(), new LongWritable(Long.parseLong(str[8])));
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	
	public static class ReduceClass extends Reducer<NullWritable, LongWritable, NullWritable, Text>
	{

		
		public void reduce(NullWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException
		{
			int count=0;
			 long sum=0;
			 
			 for(LongWritable val:values)
			 {
				 
				 sum+=val.get();
				 
				 count++;
				 
			 }
			 
			String mycount=Integer.toString(count);
			String mysum=Long.toString(sum);
			 
			 context.write(NullWritable.get(),new Text(mycount+","+mysum));	
		    	
			 
			
		}
	}

	public static void main(String[] args) throws Exception
	{
		
		Configuration conf = new Configuration();
		//conf.set("","")
		Job job = new Job(conf,"total orders and sales in a month");

		job.setJarByClass(TotalSale.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
