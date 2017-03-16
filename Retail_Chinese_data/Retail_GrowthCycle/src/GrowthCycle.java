import java.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class GrowthCycle {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    
	    	   double nov_dec=0;
	    	   double dec_jan=0;
	    	   double jan_feb=0;
	         try{
	            String[] str = value.toString().split("\t");	 
	            String pro_id=str[0];
	            long nov=Long.parseLong(str[1]);
	            long dec=Long.parseLong(str[2]);
	            long jan=Long.parseLong(str[3]);
	            long feb=Long.parseLong(str[4]);
	          
	            if (nov!=0)
	            {
	            	nov_dec= (dec-nov)*100/nov;
	            }
	            else{
	            	
	            	nov_dec= (dec-nov)*100;
	            	}
	            if(dec!=0)
	            {
	            	dec_jan=(jan-dec)*100/dec;
	            }
	            else
	            {
	            	 dec_jan=(jan-dec)*100;
	            	
	            }
	            if(jan!=0)
	            {
	            	jan_feb=(feb-jan)*100/jan;
	            }
	            else
	            {
	            	
	            	jan_feb=(feb-jan)*100;
	            }
	          
	          double averag=(nov_dec+dec_jan+jan_feb)/3;
	          
	          String cycle1=String.format("%f", nov_dec);
	          String cycle2=String.format("%f", dec_jan);
	          String cycle3=String.format("%f", jan_feb);
	          
	          String avg=String.format("%f", averag);
	          
	          String row=cycle1+","+cycle2+","+cycle3+","+avg;
	          
	            context.write(new Text(pro_id),new Text(row));
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	/*  public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
	   {
		  private IntWritable result = new IntWritable();
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      int profit = 0;
				
		         for (Text val : values)
		         {      
		        	 String str[]=val.toString().split(",");
		             int cp=Integer.parseInt(str[1]);
		             int sp=Integer.parseInt(str[0]);
		             int qty=Integer.parseInt(str[2]);
		             
		             profit+=(sp-cp)*qty;
		             
		            
		         }
		         
		         result.set(profit);  
		      context.write(key, result);
		      
		    }
	   }
	   */
	  public static void main(String[] args) throws Exception 
	  {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    Job job = new Job(conf, "growth cycles input from comprehensive sale1 ");
		    conf.set("mapreduce.output.textoutputformat.separator", ",");
		    job.setJarByClass(GrowthCycle.class);
		    job.setMapperClass(MapClass.class);
		   //job.setCombinerClass(ReduceClass.class);
		    //job.setReducerClass(ReduceClass.class);
		    
		    job.setNumReduceTasks(0);
		  
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    
		   // job.setOutputKeyClass(Text.class);
		  //  job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}