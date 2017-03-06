import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainClass
{
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	{	
		IntWritable one=new IntWritable(1);
		Text logType=new Text();
		public void map(LongWritable key,Text value,Context context)
		{
			try
			{
				String [] record=value.toString().split(" ");
				String type=record[3];
				logType.set(type);
				context.write(logType, one);
			}
			catch(Exception ex)
			{
				System.out.println(ex.getMessage());
			}
			
			
		}
	}
	
	 public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
	   {
		    private IntWritable result = new IntWritable();
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      int sum = 0;
				
		         for (IntWritable val : values)
		         {       	
		        	sum += val.get();      
		         }
		         
		      result.set(sum);		      
		      context.write(key, result);
		      
		    }
	   }
	 
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		   
		    Job job = new Job(conf, "count of each logtype");
		    job.setJarByClass(MainClass.class);
		    job.setMapperClass(MapClass.class);
		    job.setReducerClass(ReduceClass.class);
		   
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	  
}
