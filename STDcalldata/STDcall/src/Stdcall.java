
import java.io.*;
import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Stdcall {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
		Text phoneNo=new Text();
		LongWritable durationInMinutes=new LongWritable();
		
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] parts= value.toString().split(",");	 
	           if(parts[4].equals(1))
	           {
	        	   phoneNo.set(parts[0]);
	        	   String callEndTime=parts[3];
	        	   String callStartTime=parts[2];
	        	   long duration=toMillis(callEndTime)-toMillis(callStartTime);
	        	   durationInMinutes.set((duration/(60000)));
	           }
	            context.write(phoneNo,durationInMinutes);
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	      
	      private long toMillis(String date)
	      {
	    	 java.util.Date datefrm=null; 
	    	  SimpleDateFormat format =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    	  try
	    	  {
	    		  datefrm=format.parse(date);
	    		  
	    	  }
	    	  catch(Exception ex)
	    	  {
	    		  
	    		ex.printStackTrace();  
	    	  }
	    	  return datefrm.getTime();
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,LongWritable,Text,IntWritable>
	   {
		    private IntWritable result = new IntWritable();
		    
		    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		      int sum = 0;
				
		         for (LongWritable val : values)
		         {       	
		        	sum += val.get();      
		         }
		         
		      result.set(sum);		      
		      context.write(key, result);
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    conf.set("mapreduce.output.textoutputformate.separator",", ");
		    Job job = new Job(conf, "STD calls");
		    job.setJarByClass(Stdcall.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
