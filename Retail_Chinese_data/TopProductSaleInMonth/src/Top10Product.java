import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Top10Product 
{
	public static class Top10Mapper extends
	Mapper<LongWritable, Text,Text, LongWritable>
	{
		

		public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException 
		{
			String record = value.toString();
			String[] parts = record.split(";");
			String product_id=parts[5];
			int sale=Integer.parseInt(parts[8]);
		
			context.write(new Text(product_id),new LongWritable(sale));
		
		}
	}

	public static class Top5Reducer extends Reducer<Text,LongWritable,NullWritable, Text>
	 {
		
		   private TreeMap<Long, Text> topMap = new TreeMap<Long, Text>();
		   
		    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException
		    {
		      long sum = 0;
				String myvalue="";
				String mysum="";
		         for (LongWritable val : values)
		         {       	
		        	sum += val.get();      
		         }
		         myvalue=key.toString();
		         mysum=String.format("%d",sum);
		         myvalue=myvalue+","+mysum;
		         
		         topMap.put(new Long(sum),new Text(myvalue));
		         if (topMap.size() > 10)
		         {
						topMap.remove(topMap.firstKey());
					}
		    }
	      
		    protected void cleanup(Context context) throws IOException, InterruptedException 
	    	{
	    	
		    	for (Text t : topMap.descendingMap().values())
		    	{
		    		context.write(NullWritable.get(), t);
	    	
            
		    	}
	    	} 
			
		}
					
				
				
				
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Top 10 Product");
	    job.setJarByClass(Top10Product.class);
	    job.setMapperClass(Top10Mapper.class);
	    job.setReducerClass(Top5Reducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
