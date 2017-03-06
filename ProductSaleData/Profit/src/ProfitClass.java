import java.io.*;

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


public class ProfitClass {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            String sp=str[3];
	            String cp=(str[4]);
	            String qty=str[2];
	            String row=sp+","+cp+","+qty;
	            context.write(new Text(str[1]),new Text(row));
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
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
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    Job job = new Job(conf, "Profit ");
		    job.setJarByClass(ProfitClass.class);
		    job.setMapperClass(MapClass.class);
		   //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		  
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    
		   // job.setOutputKeyClass(Text.class);
		  //  job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}