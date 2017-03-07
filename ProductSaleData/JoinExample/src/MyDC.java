
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class MyDC {
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> 
	{
        
		
		private Map<String, String> abMap = new HashMap<String, String>();
		private Map<String, String> abMap1 = new HashMap<String, String>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);
			 URI[] files =DistributedCache.getCacheFiles(context.getConfiguration());
           // URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		
		    Path p1 = new Path(files[1]);
		
			if (p.getName().equals("cost-sheet.txt")) {
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split(",");
						String itm_id = tokens[0];
						String cost = tokens[1];
						abMap.put(itm_id, cost);
						line = reader.readLine();
					}
					reader.close();
				}
			if (p1.getName().equals("store_master-")) {
				BufferedReader reader = new BufferedReader(new FileReader(p1.toString()));
				String line = reader.readLine();
				while(line != null) {
					String[] tokens = line.split(",");
					String store_id = tokens[0];
					String state = tokens[2];
					abMap1.put(store_id, state);
					line = reader.readLine();
				}
				reader.close();
			}
		
			
			if (abMap.isEmpty()) {
				throw new IOException("MyError:Unable to load salary data.");
			}

			if (abMap1.isEmpty()) {
				throw new IOException("MyError:Unable to load designation data.");
			}

		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	
        	String row = value.toString();
        	String[] tokens = row.split(",");
        	String itm_id = tokens[1];
        	String store_id=tokens[0];
        	String cost = abMap.get(itm_id);
        	String state = abMap1.get(store_id);
        	String row1 = cost + "," +state; 
        	outputKey.set(row);
        	outputValue.set(row1);
      	  	context.write(outputKey,outputValue);
        }  
}
	
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = new Job(conf);
    job.setJarByClass(MyDC.class);
    job.setJobName("Map Side Join");
    job.setMapperClass(MyMapper.class);
    try
    {
    	DistributedCache.addCacheFile(new URI("cost-sheet.txt"),job.getConfiguration());
    	DistributedCache.addCacheFile(new URI("store_master-"),job.getConfiguration());
    }
    catch(Exception ex)
    {
    	System.out.println(ex.getMessage());
    }
    
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
    
    
  }
}