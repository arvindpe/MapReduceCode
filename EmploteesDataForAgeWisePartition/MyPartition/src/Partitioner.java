import java.io.*;
import java.util.Set;

import javax.lang.model.SourceVersion;
import javax.tools.Tool;

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
import org.apache.hadoop.util.ToolRunner;


public class Partitioner extends Configured implements Tool, org.apache.hadoop.util.Tool{
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	
	            String gender=str[3];
	            String age=str[2];
	            String name=str[1];
	            String salary=str[4];
	            context.write(new Text(gender),new Text(gender+","+name+","+age+","+salary));
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
	   {
		    public int max=-1;
		    public Text outputkey=new Text();
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		    {
		      max=-1;
				
		         for (Text val : values)
		         {       	
		        	String str[]=val.toString().split(",");  
		        	String mykey=str[0]+","+str[1]+","+str[2];
		        	if(Integer.parseInt(str[3])>max)
		        	{
		        		max=Integer.parseInt(str[3]);
		        		
		        	}
		        	outputkey.set(mykey);
		         }
		         
		      	      
		      context.write(outputkey, new IntWritable(max));
		      
		    }
	   }
	  
	  //partitioner class
	  public static class CaderPartitioner extends org.apache.hadoop.mapreduce.Partitioner<Text, Text>
	   {
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(",");
	         int age = Integer.parseInt(str[2]);


	         if(age<=20)
	         {
	            return 0;
	         }
	         else if(age>20 && age<=30)
	         {
	            return 1 ;
	         }
	         else
	         {
	         return 0;
	         }
			
	      }
	   }
	  public int  run(String[] arg) throws Exception
	  {
		  
		    Configuration conf = new Configuration();
		    Job job = new Job(conf, "Partion state wise");
		    job.setJarByClass(Partitioner.class);
		    job.setJobName("top salried employee ");
		    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    job.setMapperClass(MapClass.class);
		    job.setPartitionerClass(CaderPartitioner.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(3);
		    
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(arg[0]));
		    FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		    return 0;
		  }
	public static void main(String args[]) throws Exception
	{
		ToolRunner.run(new Configuration(),new Partitioner(), args);
		System.exit(0);
	}
	@Override
	public Set<SourceVersion> getSourceVersions() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public int run(InputStream arg0, OutputStream arg1, OutputStream arg2,
			String... arg3) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
	}
