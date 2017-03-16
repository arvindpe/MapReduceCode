import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;



public class SortedCost
{

	// Mapper Class	
	
	   public static class MapperClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().split(";");
	            String prodid = str[0];
	           
	            context.write(new Text(prodid), new Text(value));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	   


public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
{
	Map<Double, Set<Text>> myMap = new TreeMap<Double, Set<Text>>();
	
	


@SuppressWarnings("unchecked")
public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
   {
	   double avgcost=0;
	   String myValue="";
	   
      for (Text val : values)
      {
	         String[] token = val.toString().split(",");
	          avgcost=Double.parseDouble(token[3]);
	          String id=token[0];
	          String qty=token[1];
	          String cost=token[2];
	          myValue=id+','+qty+','+cost+','+avgcost;
     	
      }
      Set<String> appleSet = new HashSet<String>();
      appleSet.add(myValue);
      myMap.put(new Double(avgcost),(Set<Text>) new Text(myValue));


      

     ;
      myMap.put(new Double(avgcost),(Set<Text>) new Text(myValue));
     
       
   }

   
	protected void cleanup(Context context) throws IOException,
		InterruptedException 
	{
		
			for (Set<Text> t : myMap.values()) 
			{
				context.write(NullWritable.get(), (Text) t);
			}
		}
		
   
}

//Main class

public static void main(String[] args) throws Exception
{
		
		Configuration conf = new Configuration();
	    conf.set("mapreduce.output.textoutputformat.separator",",");
		Job job = new Job(conf, "avg cost of per product in ascending oreder");
		
		job.setJarByClass(SortedCost.class);
	    job.setMapperClass(MapperClass.class);
	    job.setReducerClass(ReduceClass.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}

