

import java.io.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

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


public class MySearchText {
	
	public static class SearchMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
		private Text sentence=new Text();
		IntWritable one=new IntWritable(1);
		
	      public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
	      
	      {	    
	    	  
	    	  String mySearchText = context.getConfiguration().get("myText");
	    	  String line=value.toString();
	        if(mySearchText!=null)
	        {
	        	if(line.contains(mySearchText))
	        	{
	        		
	        		sentence.set(line);
	        		context.write(sentence,one);
	        	}
	        	
	        	
	        }
	      }
	      
	     
	   }
	
	  public static class SearchReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	   {
		    private IntWritable result = new IntWritable();
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      long sum = 0;
				
		         for (IntWritable val : values)
		         {       	
		        	sum += val.get();      
		         }
		         
		      result.set((int)sum);		      
		      context.write(key, result);
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    
		    if(args.length > 2)
		    {
		    	conf.set("myText", args[2]);
		    	
		    }
		    Job job = new Job(conf, "String search");
		    job.setJarByClass(MySearchText.class);
		    job.setMapperClass(SearchMapper.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(SearchReducer.class);
		    //job.setNumReduceTasks(2);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}

