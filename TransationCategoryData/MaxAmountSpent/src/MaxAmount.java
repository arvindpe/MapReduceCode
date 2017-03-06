
import java.io.*;
import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class MaxAmount {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,DoubleWritable>
	   {
		
		DoubleWritable amount=new DoubleWritable();
		Text Id=new Text();
		
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] parts= value.toString().split(",");	 
	           
	           
	        	 double amt=Double.parseDouble(parts[3]);
	        	  Id.set(parts[2]);
	        	 amount.set(amt);
	        	 
	            context.write(Id,amount);
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	      
	     
	   }
	
	 public static class ReduceClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	   {
		    private DoubleWritable result = new DoubleWritable();
		    
		    double max=0;
		    public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
		      double sum = 0;
				
		         for (DoubleWritable val : values)
		         {       	
		        	sum += val.get();  
		        	if(sum>max)
		        	{
		        		max=sum;
		        		
		        	}
		         }
		         
		      result.set(max);		      
		      context.write(key, result);
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    conf.set("mapreduce.output.textoutputformate.separator",", ");
		    Job job = new Job(conf, "max amount spent in sales data txns1.txt");
		    job.setJarByClass(MaxAmount.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
