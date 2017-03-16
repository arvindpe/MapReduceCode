import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class AvgSort
{
	public static class MapClass extends Mapper<LongWritable,Text,DoubleWritable,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    
	    	   
	         try{
	            String[] str = value.toString().split(",");	 
	            double avg_growth=Double.parseDouble(str[4]);
	            
	            context.write(new DoubleWritable(avg_growth), value);
	         }
	         catch(Exception e)
	         {
	        System.out.println(e.getMessage());	 
	         }
	     }
	      
	   }
	      
	      public static class ReduceClass extends Reducer<DoubleWritable, Text, NullWritable, Text>
	      {
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			for (Text t : values)
			{
			context.write(NullWritable.get(), t);
			}
					
			
		}
		
	      }
	          
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		 Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    Job job = new Job(conf, "growth cycles input from output of growth cycles for sorting on avg growth ");
		    conf.set("mapreduce.output.textoutputformat.separator", ",");
		    job.setJarByClass(AvgSort.class);
		    job.setMapperClass(MapClass.class);
		   //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    
		   
		  
		    job.setMapOutputKeyClass(DoubleWritable.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	

	}

	 
	
}
