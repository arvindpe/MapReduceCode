import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MainClass {
	
	class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	{
	 public void Map(LongWritable key,Text value,Context context)
	 {
		 try{
	         String[] str = value.toString().split(",");	
	         String state=str[4];
	         int quantity=Integer.parseInt(str[2]);
	         int price=Integer.parseInt(str[3]);
	         context.write(new Text(state), new IntWritable(quantity*price));
	     
	      }
	      catch(Exception e)
	      {
	         System.out.println(e.getMessage());
	      } 
		 
		 
	 }
	}
	class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable> 
	 {
		 private IntWritable result=new IntWritable();
		 
		 public void Reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		 {
			 int sum=0;
			 for(IntWritable val:values)
			 {
				 sum+=val.get();
				 
			 }
			 
			 result.set(sum);
			context.write(key,result);
			 
		 }
	}

	 public static void main(String[] args) throws Exception 
	 {
		    Configuration conf = new Configuration();
		
		    Job job = new Job(conf, "total sales Count");
		    
		    job.setJarByClass(MainClass.class);
		    job.setMapperClass(MapClass.class);
		   // job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

}
