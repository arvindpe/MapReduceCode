import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class DriverClass extends Configured implements Tool {

	public static void main(String[] args) throws Exception
	{
		{
			ToolRunner.run(new Configuration(),new DriverClass(), args);
			System.exit(0);
		}
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = new Job(conf);
	    job.setJarByClass(DriverClass.class);
	    job.setJobName("Map Side Join of student and  result");
	    job.setMapperClass(MapClass.class);
	    try
	    {
	    	DistributedCache.addCacheFile(new URI("results.dat"),job.getConfiguration());
	    }
	    catch(Exception ex)
	    {
	    	System.out.println(ex.getMessage());
	    }
	    
	    job.setNumReduceTasks(0);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
		return 0;
	}

}
