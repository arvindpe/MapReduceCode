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


public class ProfitOfState extends Configured implements Tool, org.apache.hadoop.util.Tool{
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	
	            String itm_id=str[1];
	            String cp=str[4];
	            String sp=str[3];
	            String qty=str[2];
	            String state=str[5];
	            context.write(new Text(itm_id),new Text(cp+","+sp+","+qty+","+state));
	            
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
					String state="";
			      String itm=key.toString();
			         for (Text val : values)
			         {      
			        	 String str[]=val.toString().split(",");
			             int cp=Integer.parseInt(str[0]);
			             int sp=Integer.parseInt(str[1]);
			             int qty=Integer.parseInt(str[2]);
			            state=str[3];
			             profit+=(sp-cp)*qty;
			             
			            
			         }
			         
			         result.set(profit);  
			      context.write(new Text(itm+","+state), result);
			      
			    }
		   
	   }
	  
	  //partitioner class
	  public static class CaderPartitioner extends org.apache.hadoop.mapreduce.Partitioner<Text, Text>
	   {
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(",");
	      String state=str[3];


	         if(state.equals("MAH"))
	         {
	            return 0;
	         }
	        
	         else
	         {
	         return 1;
	         }
			
	      }
	   }
	  public int  run(String[] arg) throws Exception
	  {
		  
		    Configuration conf = new Configuration();
		    Job job = new Job(conf, "Partion state wise");
		    job.setJarByClass(ProfitOfState.class);
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
		ToolRunner.run(new Configuration(),new ProfitOfState(), args);
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
